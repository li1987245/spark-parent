package feature
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import scala.collection.mutable
/**
  * Created by jinwei on 17-12-8.
  */
object TFIDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TF-IDF").config("spark.shuffle.compress", "true").getOrCreate()
    import spark.implicits._
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.printSchema()
    //将数组类型列进行切分
    rescaledData.select(explode(rescaledData("words"))).show
    rescaledData.select("label", "features").show()
  }
}
