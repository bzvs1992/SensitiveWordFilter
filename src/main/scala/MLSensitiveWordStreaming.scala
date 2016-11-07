import java.net.InetSocketAddress

import com.gomeplus.util.Conf
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.{IDF, HashingTF}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
 * Created by wangxiaojing on 16/11/3.
 */
object MLSensitiveWordStreaming {

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

  def main(args: Array[String]) {

    // 创建spark项目
    val conf = new SparkConf().setAppName("MLSensitiveWord")
    conf.set("es.index.auto.create", "true")

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.set("es.nodes","wangxiaojingdeMacBook-Pro.local")
    val sc = new SparkContext(sparkConf)
    // 创建sparksql

    val sqlContext = new SQLContext(sc)
    // 流数据计算
    val ssc = new StreamingContext(sc, Seconds(20))
    //ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)


    // 通过流获取的数据作为测试数据使用
    val words = lines.map(x=>{
      x.replace("@","").replace("?",
        "").replace("!",
        "").replace("//",
        "").replace("\\",
        "").replace("&",
        "").trim
    }).filter(_.length>0)
      .flatMap(x=>{
      val analyzeResponse: AnalyzeResponse = client.admin.indices
        .prepareAnalyze(x).setAnalyzer("ik_smart").execute.actionGet
      val actual  = analyzeResponse.getTokens
      var sensitiveWordList = List(actual.get(0).getTerm)
      val size = actual.size()
      for(i<- 1 to size - 1){
        val now = actual.get(i).getTerm
        val before = actual.get(i - 1).getTerm.concat(now)
        sensitiveWordList = now :: before ::sensitiveWordList
        if(i < size -1 ){
          val after = before.concat(actual.get(i+1).getTerm)
          sensitiveWordList = after ::sensitiveWordList
        }

      }
      println("word is " + sensitiveWordList)
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

        val model =  NaiveBayesModel.load("test")
        val predictionAndLabel = model.transform(tfidfString)
        predictionAndLabel.printSchema()
        val wordRdd = predictionAndLabel.select("word").map(x=>{x.get(0).toString})
        val label = predictionAndLabel.select("prediction").map(x=>{x.toString()})
        data = wordRdd.zip(label)
      }
      //val out =data

      val out = data.filter(x=>{x._2.equals("[1.0]")}).map(word=>{
        println("word is " +word._1)

        val jedisCluster = JedisClient.getJedisCluster()
        val long = jedisCluster.sadd(ikMain,word._1)
        jedisCluster.publish(ikMain,word._1)
        (word,long)
      })

      out
    })

    words.print()
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
