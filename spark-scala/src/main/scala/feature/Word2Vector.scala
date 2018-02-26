package feature

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}
import com.databricks.spark.xml._

/**
  * http://www.sogou.com/labs/resource/ca.php
  * 用户名：li_1987245@163.com
  * 密码：(!be&eUfb&ss458s
  * iconv  -c  -f GB2312 -t UTF-8 filename > filename.txt
  * xml data source read https://github.com/databricks/spark-xml
  * Created by jinwei on 17-12-12.
  */
object Word2Vector {

  case class NewsType(url: String, tag: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().appName("TF-IDF").config("spark.shuffle.compress", "true").getOrCreate()
    import spark.implicits._
    val newsDF = spark.read.format("com.databricks.spark.xml").option("rowTag", "doc").load("/data/decision/news_tensite_xml.txt")
    val typeDF = spark.read.text("/data/decision/SogouTCE.txt").map(line => {
      val Array(url, tag) = line.getString(0).split("\t")
      NewsType(url, tag)
    })
    typeDF.where(typeDF("url") contains "news.sohu").count()
    newsDF.map(row => {
      val regex = "http(s?)://(.*?).com.*".r
      val url: String = "http://news.sohu.com/20120612/n345428229.shtml"
      val name = url match {
        case regex(s, n) => n
        case _ => None
      }
      name.toString
    }).distinct.show
    newsDF.join(typeDF, newsDF("url") contains typeDF("url")).count()
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }
  }
}
