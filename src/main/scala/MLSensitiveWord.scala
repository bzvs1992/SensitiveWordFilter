import java.net.InetSocketAddress

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkContext

import com.gomeplus.util.Conf
import org.apache.spark.ml.feature.{IDF, Tokenizer, HashingTF}

import org.apache.spark.ml.classification.{NaiveBayesModel, NaiveBayes}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisCluster, HostAndPort}

//import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
/**
 * Created by wangxiaojing on 16/10/24.
 */

object MLSensitiveWord {

  val loggers = LoggerFactory.getLogger("MLSensitiveWord")
  val hdfsPath = "sensitiveFilter"
  // 敏感词的在redis中的主key
  val ikMain = "ik_main"

  // 生成es 连接
  val config = new Conf
  val esHostname: Array[String] = config.getEsHostname.split(",")
  val clusterName: String = config.getEsClusterName
  var inetSocketAddress: InetSocketAddress = null
  for (hostname <- esHostname) {
    inetSocketAddress = new InetSocketAddress(hostname, 9300)
  }
  val settings: Settings = Settings.settingsBuilder.put("cluster.name", clusterName).build
  val client: TransportClient = TransportClient.builder.settings(settings).
    build.addTransportAddress(new InetSocketTransportAddress(inetSocketAddress))


  def main (args: Array[String]){

    val conf = new SparkConf().setAppName("MLSensitiveWord")
    conf.set("es.index.auto.create", "true")

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.set("es.nodes","wangxiaojingdeMacBook-Pro.local")
    val sc = new SparkContext(sparkConf)

    //创建Dataframe
    val sqlContext = new SQLContext(sc)

    // 通过hdfs读取的数据作为和es内的数据作为训练数据，训练模型

    //敏感词的lable设置为1.0，非敏感词的lable为0.0
    // 获取es中敏感词数据
    val sensitiveWordIndex = sc.esRDD("gome/word")
    val sensitiveWord = sensitiveWordIndex.map(x=>{
      val words = x._2.getOrElse("word","word").toString
      (Array(words),1.0)})

    // 获取hdfs内的数据作为非敏感词数据进行训练模型
    val unSensitiveWordLine =  sc.textFile(hdfsPath)
    val unSensitiveWords = unSensitiveWordLine.filter(_.size>0).flatMap(x=>{
      val analyzeResponse: AnalyzeResponse = client.admin.indices
        .prepareAnalyze(x).setAnalyzer("ik_smart").execute.actionGet
      val actual  = analyzeResponse.getTokens
      var sensitiveWordList = List(actual.get(0).getTerm)
      for(i<- 1 to actual.size() - 1){
        sensitiveWordList = actual.get(i).getTerm ::sensitiveWordList
      }
      sensitiveWordList
    }).map(x=>{(Array(x),0.0)})

    // 生成tfidf 疵品
    //val trainDataFrame = sensitiveWord.union(unSensitiveWords)
    //创建一个(word，label) tuples
    val trainDataFrame = sqlContext.createDataFrame(sensitiveWord.union(unSensitiveWords)).toDF("word","label")
    val hashingTF = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(20)
    val tf = hashingTF.transform(trainDataFrame)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(tf)
    val tfidf = idfModel.transform(tf)

    tfidf.printSchema()
    tfidf.take(5).foreach(println)


    // 使用mlib方式
    //val trainDataFrameMllib = LabeledPoint(sensitiveWord.union(unSensitiveWords))
    //NaiveBayes.train(trainDataFrameMllib, lambda = 1.0, modelType = "multinomial")

    // 进行贝叶斯计算
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial")
    val model = nb.fit(tfidf)

    //model.save("test")
    NaiveBayesModel.load("test")

    sensitiveWord.map((x)=>{print(x._1 + " " + x._2)
    x}).collect()


    // 流数据计算
    val ssc = new StreamingContext(sc, Seconds(20))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)


    // 通过流获取的数据作为测试数据使用
    val words = lines.filter(_.size>0).flatMap(x=>{
      val analyzeResponse: AnalyzeResponse = client.admin.indices
        .prepareAnalyze(x).setAnalyzer("ik_smart").execute.actionGet
      val actual  = analyzeResponse.getTokens
      var sensitiveWordList = List(actual.get(0).getTerm)
      for(i<- 1 to actual.size() - 1){
        sensitiveWordList = actual.get(i).getTerm ::sensitiveWordList
      }
      sensitiveWordList
    }).map(x=>{(Array(x),0.0)})



    val train = words.transform(x=>{
      val dataStreaming = sqlContext.createDataFrame(x).toDF("word","label")
      var data = dataStreaming.rdd.map(x=>((x.get(0).toString,"ggg")))
      if(dataStreaming.count() > 0){
        //val data = x.union(sensitiveWord)
        val hashingTFString = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(20)
        val tfString = hashingTFString.transform(dataStreaming)
        val idfString = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModelString = idfString.fit(tfString)
        val tfidfString = idfModelString.transform(tfString)

        val predictionAndLabel = model.transform(tfidfString)
        predictionAndLabel.printSchema()
        val wordRdd = predictionAndLabel.select("word").map(x=>{x.get(0).toString})
        val label = predictionAndLabel.select("prediction").map(x=>{x.toString()})
        data = wordRdd.zip(label)
      }
      val out = data.filter(x=>{x._2.equals("[1.0]")}).map(word=>{
        println("word is " +word._1)
        val long = getJedisCluster.sadd(ikMain,word._1)
        (word,long)
      })
      out
    })

    train.print()
    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * 创建redis的连接，
   * */
  def getJedisCluster(): JedisCluster ={

    val config = new Conf
    val redisHost = config.getRedisHosts.split(";")
    // 获取redis地址
    val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
    redisHost.foreach(x=>{
      val redisHostAndPort = x.split(":")
      jedisClusterNodes.add(new HostAndPort(redisHostAndPort(0),redisHostAndPort(1).toInt))
    })

    val redisTimeout = 3000
    val poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig
    poolConfig.setJmxEnabled(false)
    val jc:JedisCluster = new JedisCluster(jedisClusterNodes, redisTimeout, 10, poolConfig)
    jc
  }
}
