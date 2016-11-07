import java.net.InetSocketAddress

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.fs.Path
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

    //val Array(zkQuorum, group, topics, numThreads) = args
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
      words
     })

    //println(sensitiveWord.count())
    //sensitiveWord.foreach(println)
    val size = sensitiveWord.cache().count().toDouble

    // 获取hdfs内的数据作为非敏感词数据进行训练模型
    val unSensitiveWordLine =  sc.textFile(hdfsPath).map(x=>{
      x.replace("@",
        "").replace("?",
        "").replace("!",
        "").replace("//",
        "").replace("\\",
        "").replace("&",
        "").replace("@",
        "").trim
    })
    val unSensitiveWords = unSensitiveWordLine.filter(_.size>0).flatMap(x=>{
      val analyzeResponse: AnalyzeResponse = client.admin.indices
        .prepareAnalyze(x).setAnalyzer("ik_smart").execute.actionGet
      val actual  = analyzeResponse.getTokens
      var sensitiveWordList = List(actual.get(0).getTerm)
      for(i<- 1 to actual.size() - 1){
        sensitiveWordList = actual.get(i).getTerm ::sensitiveWordList
      }
      sensitiveWordList
    }).distinct(10)//.map(x=>{(Array(x),0.0)})

    println(unSensitiveWords.count())
    //println(unSensitiveWords.count())

    //创建一个(word，label) tuples

    val sensitiveWord_2 = sensitiveWord.map(x=>{(Array(x),1.0)})
    val unSensitiveWords_2 = unSensitiveWords.subtract(sensitiveWord).map(x=>{(Array(x),0.0)})
    //val unSensitiveWords_2 = unSensitiveWords.map(x=>{(Array(x),0.0)})

    val rdd = sensitiveWord_2.union(unSensitiveWords_2)
    val trainDataFrame = sqlContext.createDataFrame(rdd).toDF("word","label")
    val hashingTF = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(2000)
    val tf = hashingTF.transform(trainDataFrame)
    tf.printSchema()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(tf)
    val tfidf = idfModel.transform(tf)
    //tfidf.show(10)

    tfidf.printSchema()
    //tfidf.take(5).foreach(println)

    // 使用mlib方式
    //val trainDataFrameMllib = LabeledPoint(sensitiveWord.union(unSensitiveWords))
    //NaiveBayes.train(trainDataFrameMllib, lambda = 1.0, modelType = "multinomial")

    // 进行贝叶斯计算
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial")
    val model = nb.fit(tfidf)
    // 保存贝叶斯模型
    val path = new Path("test")
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(path)

    // 保存模型
    model.write.overwrite().save("test")
    //model.save("test")

    // 模型验证
    val loadModel = NaiveBayesModel.load("test")
    val out = loadModel.transform(tfidf)
    val testData = out.select("word","prediction","label").filter("prediction=label").count().toDouble
    val testDataFalse_data = out.select("word","prediction","label").filter("prediction!=label")
    val testDataFalse = testDataFalse_data.count().toDouble
    //testDataFalse_data.foreach(println)
    val t = testDataFalse_data.filter("prediction = 1.0 and label = 0.0" )
    val t1 = testDataFalse_data.filter("prediction = 0.0 and label = 1.0" )

    val all = out.count().toDouble
    val num = testData/all*100
    val s = out.filter("prediction = 1.0").count()
    val ttof = t.count().toDouble
    val ftot = t1.count().toDouble
    val ttot = ttof/size*100

    out.select("word","prediction","label").filter("prediction=label and label=1.0").show(100)
    loggers.info(" 最终输出敏感词个数 ：" +  s + "实际上敏感词个数 " + size)
    loggers.info("打标错误，正常词打成敏感词个数："+ ttof + " 敏感词打成正常词个数：" + ftot+ "错误判断敏感词的概率" + ttot)
    loggers.info("总计： " + all + "正确 ： " + testData + " 错误个数：" + testDataFalse + " 准确率 :" + num + "%")

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
