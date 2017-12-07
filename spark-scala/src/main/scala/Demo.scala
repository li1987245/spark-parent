import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import scala.collection.mutable.Set

import scala.util.Random

/**
  * Created by jinwei on 17-10-31.
  */
object Demo {
  case class Movie(id:Int,name:String,tag:String)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
//      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val movieDF = spark.read.option("header", true).csv("/user/ljw/data/recommend/movies.csv")
    val moviesDF = movieDF.flatMap(x => {
      val id = x.getString(0)
      val name = x.getString(1)
      val tags = x.getString(2)
      val arr = tags.split("\\|")
      for (i <- 0 until arr.size)
        yield Movie(id.toInt, name, arr.apply(i))
    })
    val tagDF= moviesDF.mapPartitions(x=>{
      var result:List[String] = List()
      while (x.hasNext){
        result=result.::(x.next().tag)
      }
      result.iterator
    })
    tagDF.randomSplit(Array(1,2,2),1000)(0).count()
  }
}
