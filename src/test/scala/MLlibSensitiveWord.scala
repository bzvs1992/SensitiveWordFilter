import java.net.InetSocketAddress

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import com.gomeplus.util.Conf
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IDF, HashingTF}

import org.apache.spark.ml.classification.{NaiveBayesModel, NaiveBayes}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisCluster, HostAndPort}

import org.apache.spark.sql.SQLContext
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
/**
 * Created by wangxiaojing on 16/10/24.
 */

object MLlibSensitiveWord {

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
      (words,1.0)})
    //println(sensitiveWord.count())
    //sensitiveWord.foreach(println)
    sensitiveWord.cache().count()

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
    }).distinct(10).map(x=>{(x,0.0)})

    println(unSensitiveWords.count())
    //println(unSensitiveWords.count())

    //创建一个(word，label) tuples
    /*
    val rdd = sensitiveWord.union(unSensitiveWords)

    val hashingTF = new HashingTF(20)
    hashingTF.transform(rdd)

    val trainDataFrame = sqlContext.createDataFrame(rdd).toDF("word","label")
    //val hashingTF = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(20)
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
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("bernoulli")
    val model = nb.fit(tfidf)

    // 保存贝叶斯模型
    val path = new Path("test");
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(path)

    // 保存模型
    model.write.overwrite().save("test")
    //model.save("test")

    // 模型验证
    val loadModel = NaiveBayesModel.load("test")
    val out = loadModel.transform(tfidf)
    val tranformData = out.select("word","prediction","label").filter("prediction=label").count()
    val tanformDataFalse = out.select("word","prediction","label").filter("prediction!=label").count()
    val all = out.count()
    val num = tranformData/(all)
    println("总计： " + all + "正确 ： " + tranformData + " 错误个数：" + tanformDataFalse + "准确率 " + num)
**/
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
