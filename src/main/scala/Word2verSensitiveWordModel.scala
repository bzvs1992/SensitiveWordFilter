import java.net.InetSocketAddress

import com.gomeplus.util.Conf
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.feature._
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
    //命令行参数解析
    conf.parse(args)
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    sparkConf.set("es.nodes","wangxiaojingdeMacBook-Pro.local")
    sparkConf.set("es.index.auto.create", "true")
    val sc = new SparkContext(sparkConf)

    //创建Dataframe
    val sqlContext = new SQLContext(sc)

    // 通过hdfs读取的数据作为和es内的数据作为训练数据，训练模型

    //敏感词的lable设置为1，非敏感词的lable为0
    // 获取es中敏感词数据
    val sensitiveWordIndex = sc.esRDD("gome/word")
    val sensitiveWord = sensitiveWordIndex.map(x=>{
      val words = x._2.getOrElse("word","word").toString
     words})
    val sensitiveWordcount = sensitiveWord.cache().count()

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
    }).subtract(sensitiveWord).distinct(10).map(x=>{(Array(x),0.0)})

    val Array(trainingData, testData) = unSensitiveWords.randomSplit(Array(0.2, 0.8))

    val sensitiveWords = sensitiveWord.map(x=>{
      val analyzeResponse: AnalyzeResponse = client.admin.indices
        .prepareAnalyze(x).setAnalyzer("ik_max_word").execute.actionGet
      val actual  = analyzeResponse.getTokens
      var sensitiveWordList = List(actual.get(0).getTerm)
      for(i<- 1 to actual.size() - 1){
        sensitiveWordList = actual.get(i).getTerm ::sensitiveWordList
      }
      (sensitiveWordList.toArray,1.0)})

    val sensitiveWords_only = sensitiveWord.map(x=>{(Array(x),1.0)})
    val trainDataFrame = sqlContext.createDataFrame(sensitiveWords.union(unSensitiveWords)).toDF("word","label")
/*
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainDataFrame)

    val word2Vec = new Word2Vec()
      .setInputCol("word")
      .setOutputCol("features")
      .setVectorSize(1)
      .setMinCount(1)

    val word2VecModel = word2Vec.fit(trainDataFrame)*/

    val hashingTF = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(20)
    val tf = hashingTF.transform(trainDataFrame)
    tf.printSchema()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(tf)
    val tfidf = idfModel.transform(tf)

    tfidf.printSchema()

    val layers = Array[Int](20,10,9,2)
    val mlpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(512)
      .setSeed(1234L)
      .setMaxIter(128)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    /*
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer,word2Vec,mlpc,labelConverter))
    val model = pipeline.fit(trainDataFrame)

    model.write.overwrite().save("dir")

    val result = PipelineModel.load("dir").transform(trainDataFrame)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val predictionAccuracy = evaluator.evaluate(result)
    */

    val model = mlpc.fit(tfidf)

    // 保存贝叶斯模型
    val path = new Path("dir")
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    hdfs.deleteOnExit(path)

    model.write.overwrite().save("dir")

    /********************开始测试******************/
    if(args.length>0 && args(0).equals("test")){
      val testDataFrame = sqlContext.createDataFrame(sensitiveWords_only).toDF("word","label")
      val testtdf = hashingTF.transform(testDataFrame)
      val testidfModel = idf.fit(testtdf)
      val testTfidf = testidfModel.transform(testtdf)

      val result = model.transform(testTfidf)
      result.printSchema()

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("precision")
      val predictionAccuracy = evaluator.evaluate(result)

      //val out = model.transform(trainDataFrame)
      val label_1 = result.select("word","prediction","label").filter("prediction=label and label=1.0")
      val tranformData =label_1.count().toDouble
      val tanformDataFalse = result.select("word","prediction","label").filter("prediction!=label").count().toDouble
      val all = result.count().toDouble
      val t = result.select("word","prediction","label").filter("prediction=label").filter("prediction=0.0").count()
      result.select("word","prediction","label").filter("prediction!=label").show(6000)
      label_1.show(10000)
      val num = tranformData/(sensitiveWordcount.toDouble)
      println("正确判断非敏感词个数 :" + t + " 正确判断敏感词的个数  " + tranformData)
      println("总词汇量： " + all + "正确 ： " + tranformData + " 错误个数：" + tanformDataFalse + "准确率 " + num)
      println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")
    }
  }
}
