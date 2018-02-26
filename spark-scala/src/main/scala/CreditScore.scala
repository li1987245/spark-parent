import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import scala.collection.mutable.Set

/**
  * 信用评分
  *
  */
object CreditScore {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", "")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val movieDF = spark.read.option("header", true).csv("/user/ljw/data/recommend/movies.csv")
    val moviesDF = movieDF.flatMap(x => {
      val id = x.getString(0)
      val name = x.getString(1)
      val tags = x.getString(2)
      val arr = tags.split("\\|")
      for (i <- 0 until arr.size)
        yield Some(id.toInt, name, arr.apply(i))
    })
  }
}
