package com.gomeplus.sensitive

import java.net.{SocketException, InetSocketAddress}

import com.alibaba.fastjson.JSON
import com.gomeplus.util.Conf
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.spark._
import org.slf4j.LoggerFactory

import scalaj.http.Http

/**
 * Created by wangxiaojing on 16/10/24.
 */

object MLSensitiveWord {

  val loggers = LoggerFactory.getLogger("MLSensitiveWord")
  val hdfsPath = "trainingData"
  // 敏感词的在redis中的主key
  val ikMain = "ik_main"
  val modelDir = "modelDir"
  def main (args: Array[String]){

    //参数解析
    val config = new Conf
    config.parse(args)
    val esHostNames: Array[String] = config.getEsHostname.split(",")
    loggers.debug(esHostNames.toString)
    // 生成es 连接
    val url = "http://"+ esHostNames(0) + "/_analyze"
   // 创建sparkContext
    val sparkConf = new SparkConf()
    sparkConf.set("es.nodes",config.getEsHostname)
    sparkConf.set("es.index.auto.create", "true")
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

    val size = sensitiveWord.cache().count().toDouble

    // 获取hdfs内的数据作为非敏感词数据进行训练模型
    val unSensitiveWordLine =  sc.textFile(hdfsPath).map(x=>{
      x.replace("@", "")
        .replace("?", "")
        .replace("!", "")
        .replace("//", "")
        .replace("\\", "")
        .replace("&", "")
        .replace("@", "")
        .replace("%","")
        // 中文字符的替换
        .replace("？","")
        .replace("】","")
        .replace("”","'")
        .replace("“","'")
        .trim
    })
    val unSensitiveWords = unSensitiveWordLine.filter(_.size>0).flatMap(x=>{
      var sensitiveWordList = List(new String)
      try{
        val result = Http(url)
          .param("pretty","true")
          .param("analyzer","ik_smart")
          .param("text",x)
          .asString.body
        val actual = JSON.parseObject(result).getJSONArray("tokens")
        if(actual!= null){
          for(i <- 0 until  actual.size()){
            val thisWord = actual.getJSONObject(i).getString("token")
            //将长度为1的语句不作为敏感词汇处理
            if(thisWord.size > 1){
              sensitiveWordList = thisWord ::sensitiveWordList
            }
            //sensitiveWordList = thisWord ::sensitiveWordList
            //目的防止由于分词破坏了原有的字符意义
            // 如果是第一个单词
            /* if(i < (actual.size() - 2)){
               // 当前单词和下一个单词拼接成新的字符串
               val nextWord =  thisWord + actual.getJSONObject(i+1).getString("token")
               sensitiveWordList = nextWord ::sensitiveWordList
             } else if(0 < i & i < (actual.size() - 2)){
               // 当前单词和下一个单词拼接成新的字符串
               val nextWord =  thisWord + actual.getJSONObject(i+1).getString("token")
               // 当前单词和前后单词拼接成新的字符串
               val threeWord = actual.getJSONObject(i-1).getString("token") + thisWord + actual.getJSONObject(i+1).getString("token")
               sensitiveWordList = nextWord :: threeWord ::sensitiveWordList
             }*/

            /************************如果是一个字则将它与后面的字合并到一起****************************************/
          }
        }
      }catch {
        case e:SocketException =>{
        loggers.info("Unexpected end of file from server: " + x)
        }
      }
      sensitiveWordList
    }).filter(x=>{x!=null}).distinct(10)

    //创建一个(word，label) tuples
    val sensitiveWord_2 = sensitiveWord.map(x=>{(Seq(x),1.0)})
    val unSensitiveWords_2 = unSensitiveWords.subtract(sensitiveWord).map(x=>{(Seq(x),0.0)})
    //val Array(t1 ,t2,t3) = unSensitiveWords_2.randomSplit(Array(0.2,0.4,0.4))
    val rdd = sensitiveWord_2.union(unSensitiveWords_2)
    val trainDataFrame = sqlContext.createDataFrame(rdd).toDF("word","label")
    val Array(trainData,testData) = trainDataFrame.randomSplit(Array(0.8,0.2))
    val hashingTF = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(200)
    val tf = hashingTF.transform(trainDataFrame)
    tf.printSchema()
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(tf)
    val tfidf = idfModel.transform(tf)

    tfidf.printSchema()

    if(args.length>0 && args(0).equals("training")) {
      // 进行贝叶斯计算
      val nb = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial")
      val model = nb.fit(tfidf)
      // 保存贝叶斯模型
      val path = new Path(modelDir)
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      hdfs.deleteOnExit(path)
      // 保存模型
      model.write.overwrite().save(modelDir)
    }else if(args.length>0 && args(0).equals("test")){
    /****************************开始测试*****************************/
      loggers.info("Start test this model:")
      val unSensitiveWordsCount = unSensitiveWords.count()
      val testRdd = sensitiveWord_2.union(unSensitiveWords_2)
      val testDataFrame = sqlContext.createDataFrame(testRdd).toDF("word","label")
      val testTdf = hashingTF.transform(testData)
      val testIdfModel = idf.fit(testTdf)
      val testTfIdf = testIdfModel.transform(testTdf)

      // 模型验证
      val loadModel = NaiveBayesModel.load(modelDir)
      val out = loadModel.transform(testTfIdf)
      val trueData = out.select("word","prediction","label").filter("prediction=label")
      val falseData = out.select("word","prediction","label").filter("prediction!=label")
      val falseDataNum = falseData.count().toDouble
      val trueDataNum = trueData.count().toDouble

      //testDataFalse_data.foreach(println)
      val truePositive = trueData.filter("prediction = 1.0 and label = 1.0 " )
      val trueNegative = trueData.filter("prediction = 0.0 and label = 0.0" )
      val falsePositive = falseData.filter("prediction = 1.0 and label = 0.0 ")
      val falseNegative = falseData.filter("prediction = 0.0 and label = 1.0 ")

      val tp = truePositive.count().toDouble
      val tn = trueNegative.count().toDouble
      val fp = falsePositive.count().toDouble
      val fn = falseNegative.count().toDouble

      val all = falseDataNum +  trueDataNum
      //准确率
      val accuracy  = trueDataNum/all
      // 精准率
      val precision =  tp /(tp + fp)
      //召回率
      val recall = tp /(tp + fn)
      val num = size + unSensitiveWordsCount
      val score = precision * recall/2/(recall + precision)


      //truePositive.select("word").map(x=>{x.getList(0).get(0).toString}).saveAsTextFile("truePositive")
      //trueNegative.select("word").map(x=>{x.getList(0).get(0).toString}).saveAsTextFile("trueNegative")
      //falsePositive.select("word").map(x=>{x.getList(0).get(0).toString}).saveAsTextFile("falsePositive")
      //falseNegative.select("word").map(x=>{x.getList(0).get(0).toString}).saveAsTextFile("falseNegative")
      out.show(1000)
      loggers.info("正确判断：正确判断敏感词个数 ：" +  tp + "正确判断非敏感词个数" + tn)
      loggers.info("打标错误，正常词打成敏感词个数："+ fp + " 敏感词打成正常词个数：" + fn )
      loggers.info( "输入数据：总计 " + num + " 实际上敏感词个数 " + size + "  非敏感词个数 ： " + unSensitiveWordsCount )
      loggers.info("输出数据 ：总计： " + all + "正确 ： " + trueDataNum + " 错误个数：" + falseDataNum )
      loggers.info("精准率" + precision + " 准确率 :" + accuracy + " 召回率 ：" + recall + " F1 score: " + score)

    }
  }
}
