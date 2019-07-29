package structured

import java.io.File
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object StreamingDemo {
  case class Words(value: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    //    val directory = new File(""); //参数为空
//    System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\spark-scala\\winutil")
    val sparkConf = new SparkConf()
    sparkConf.setAppName("structured streaming demo").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    import spark.implicits._

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" ")).map(x => Words(x, System.currentTimeMillis()))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()
//    val windowedCounts = words
//      .withWatermark("timestamp", "10 seconds")
//      .groupBy(
//        functions.window(words.col("timestamp"), "10 seconds", "5 seconds"),
//        words.col("word"))
//      .count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
