package com.gomeplus.sensitive

import java.net.{SocketException, InetSocketAddress}

import com.alibaba.fastjson.{JSONObject, JSONException, JSON}
import com.gomeplus.util.Conf
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.spark._
import org.slf4j.LoggerFactory
import scalaj.http.Http

/**
 * Created by wangxiaojing on 16/11/3.
 */
object MLSensitiveWordStreaming {

  val loggers = LoggerFactory.getLogger("MLSensitiveWord Streaming")
  val ikMain = "ik_main_confirm"
  val modelDir = "modelDir"
  def main(args: Array[String]) {
    // 参数解析
    val config = new Conf()
    config.parse(args)
    val redisHost = config.getRedisHosts
    val esHostNames: Array[String] = config.getEsHostname.split(",")
    loggers.debug(esHostNames.toString)
    // 生成es 连接
    val url = "http://"+ esHostNames(0) + "/_analyze"
    val zkQuorum = config.getZkServers
    val topics = config.getTopic
    val numThreads = config.getStreamingNumThreads
    val jsonText = config.getJsonText.split(",")

    // 创建spark项目
    val sparkConf = new SparkConf()
    sparkConf.set("es.nodes",config.getEsHostname)
    sparkConf.set("es.index.auto.create", "true")
    val sc = new SparkContext(sparkConf)
    // 创建sparksql

    val sqlContext = new SQLContext(sc)
    // 流数据计算
    val ssc = new StreamingContext(sc, Seconds(10))
    //ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, "SensitiveFilterFound", topicMap).map(_._2).filter(_.size>0)

    // 通过流获取的数据作为测试数据使用
    val contents = lines.map(x=>{
      var jsonObject= new JSONObject()
      try {
         jsonObject = JSON.parseObject(x)
      }catch {
        case ex:JSONException => {
          jsonObject = null
          ex.printStackTrace()
        }
      }
      if(jsonObject != null){
        for(i<- 0 to jsonText.size-2){
          jsonObject = jsonObject.getJSONObject(jsonText(i))
        }
        val text = jsonObject.getString(jsonText.last)
        loggers.debug("input text is : " + text)
        text.replace("@","")
          .replace("?", "")
          .replace("!", "")
          .replace("//", "")
          .replace("\\", "")
          .replace("&", "")
          // 中文字符的替换
          .replace("？","")
          .replace("】","")
          .replace("”","'")
          .replace("“","'")
          .trim
      }else{
        val text =""
        text
      }
    })

    val words = contents.filter(_.length>0)
      .flatMap(x=>{
        var sensitiveWordList = List(new String)
        try{
          val result = Http(url)
            .param("pretty","true")
            .param("analyzer","ik_smart")
            .param("text",x)
            .asString.body
          val actual = JSON.parseObject(result).getJSONArray("tokens")
          if(actual != null){
            for(i <- 0 until  actual.size()){
              val thisWord = actual.getJSONObject(i).getString("token")
              //将长度为1的语句不作为敏感词汇处理
              if(thisWord.size > 1){
                sensitiveWordList = thisWord ::sensitiveWordList
              }
            }
          }
        }catch {
          case e:SocketException =>{
            loggers.info("Unexpected end of file from server: " + x)
          }
        }
        sensitiveWordList
    }).map(x=>{(Seq(x),0.0)})



    val train = words.transform(x=>{
      val dataStreaming = sqlContext.createDataFrame(x).toDF("word","label")
      var data = dataStreaming.rdd.map(x=>((x.getString(0))))
      if(dataStreaming.count() > 0){
        val hashingTFString = new  HashingTF().setInputCol("word").setOutputCol("rawFeatures").setNumFeatures(200)
        val tfString = hashingTFString.transform(dataStreaming)
        val idfString = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModelString = idfString.fit(tfString)
        val tfidfString = idfModelString.transform(tfString)

        val model =  NaiveBayesModel.load(modelDir)
        val predictionAndLabel = model.transform(tfidfString)
        predictionAndLabel.printSchema()
        data = predictionAndLabel.filter("prediction=1.0").select("word").map(x=>{x.getList(0).get(0).toString})
      }

      val out = data.map(word=>{
        val jedisCluster = JedisClient.getJedisCluster(redisHost)
        val reply = jedisCluster.sadd(ikMain,word)
        if(reply == 1){
          jedisCluster.publish(ikMain,word)
        }
        (reply,word)
      }).filter(x=>x._1 == 1).map(word=>{
        //loggers.debug("sensitiveword is: " + word._2)
        ("word",word._2)})
      //将新的敏感词存入ES
      out.saveToEs("gome/word1")
      out
    })

    train.print()
    contents.transform(x=>{
      x.saveAsTextFile(topics)
      x
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }

}
