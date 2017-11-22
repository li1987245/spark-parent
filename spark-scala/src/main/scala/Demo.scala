import com.star.util.{IDUtil, RandomValue}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  *
  * Created by jinwei on 17-10-31.
  */
object Demo {
  def main(args: Array[String]): Unit = {
    val data = List("Scala,1,2", "Hadoop,1,2", "Spark,1,2")
    import scala.collection.mutable.Set
    //    var movieSet=Set("Hitch","Poltergeist")
    //    movieSet +="Shrek"
    //1条to多条
    //    data.flatMap(_.toList).foreach(println)
    //    val g = new IDUtil()
    //    val rdd = data.map {
    //      x => {
    //        val arr = x.split(",")
    //        val rand = Random.nextInt(2)
    //        if (rand == 0)
    //          arr(1) = "-"
    //        else
    //          arr(1) = g.generate()
    //        arr(2) = RandomValue.getChineseName()
    //        arr.mkString("\t")
    //      }
    //    }
    //    rdd.foreach(println)
  }
}
