import java.net.InetSocketAddress

import com.gomeplus.util.Conf
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.spark._


/**
 * Created by wangxiaojing on 16/10/27.
 */
object Word2verSensitiveWordModel {

  val hdfsPath = "sensitiveFilter"
  // 生成es 连接
  val conf = new Conf
  val esHostname: Array[String] = conf.getEsHostname.split(",")
  val clusterName: String = conf.getEsClusterName
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

    //敏感词的lable设置为1，非敏感词的lable为0
    // 获取es中敏感词数据
    val sensitiveWordIndex = sc.esRDD("gome/word")
    val sensitiveWord = sensitiveWordIndex.map(x=>{
      val words = x._2.getOrElse("word","word").toString
      (Array(words),1.0)})
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
    }).distinct(10).map(x=>{(Array(x),0.0)})

    val trainDataFrame = sqlContext.createDataFrame(sensitiveWord.union(unSensitiveWords)).toDF("word","label")

    val word2Vec = new Word2Vec()
      .setInputCol("word")
      .setOutputCol("features")
      .setVectorSize(20)
      .setMinCount(1)

    val word2VecModel = word2Vec.fit(trainDataFrame)


    val layers = Array[Int](20,6,5,2)
    val mlpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(512)
      .setSeed(1234L)
      .setMaxIter(128)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(word2Vec,mlpc))
    val model = pipeline.fit(trainDataFrame)

    model.write.overwrite().save("dir")

    val result = PipelineModel.load("dir").transform(trainDataFrame)
    result.printSchema()
    result.take(4).foreach(println)

    val out = model.transform(trainDataFrame)
    val tranformData = out.select("word","prediction","label").filter("prediction=label").count().toDouble
    val tanformDataFalse = out.select("word","prediction","label").filter("prediction!=label").count().toDouble
    val all = out.count().toDouble
    val num = tranformData/(all)
    println("总计： " + all + "正确 ： " + tranformData + " 错误个数：" + tanformDataFalse + "准确率 " + num)

  }
}
