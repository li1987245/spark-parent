package recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * http://spark.apache.org/docs/latest/configuration.html
  * Created by jinwei on 17-11-1.
  */
object MovieRecommend {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("movie recommend")
    val sc = new SparkContext(conf)
    //1.6
    val sqlContext = new SQLContext(sc)
    //2.0
    val spark = SparkSession.builder().appName("movie recommend").config("spark.shuffle.compress", "true").getOrCreate()
    import spark.implicits._
    val movies = spark.read.option("header", true).csv("hdfs://localhost:9000//data/recommend/movies.csv")
    //val movies = spark.read.csv("file:///home/jinwei/data/recommend/Recommend-movies-master/ml-latest-small/movies.csv")
    movies.printSchema()
    //mkString把数组按照指定分隔符组成字符串
    println(movies.first().mkString("\t"))
    val rdd = movies.flatMap {
      x => {
        var resut: List[Any] = List()
        val seq = x.toSeq
        val t = seq.apply(2).toString.split("\\|")
        for (i <- 0 until t.length)
          yield resut.++(List(seq(0), seq(1), t(i)))
      }
    }
    println(rdd.toDF().first())


  }
}
