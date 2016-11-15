
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{IDF, Tokenizer, HashingTF}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object TfIdfExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TfIdfExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val sentenceData = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(200)
    val featurizedData = hashingTF.transform(wordsData)

    featurizedData.select("rawFeatures").foreach(println)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    //rescaledData.show(5)
    rescaledData.select("features", "label").take(3).foreach(println)
    // $example off$
  }
}
