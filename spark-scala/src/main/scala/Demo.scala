import com.star.util.{IDUtil, RandomValue}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Set

import scala.util.Random

/**
  *
  * Created by jinwei on 17-10-31.
  */
object Demo {

  case class UserInfo(name: String, idCard: String, province: String, city: String, agg: Int)

  def parse_User(line: String): Option[UserInfo] = {
    val user_pattern = """^(\d{15}|\d{18})$""".r
    val arr = line.split("\t")
    val idCard = arr(1)
    val name = arr(2)
    idCard match {
      case "-" => None
      case user_pattern(user) => {
        if (user.length == 18) {
          val province = user.substring(0, 2)
          val city = user.substring(0, 4)
          val agg = user.substring(6, 10)
          if (!agg.startsWith("19") && !agg.startsWith("20"))
            None
          else
            Some(UserInfo(name, idCard, province, city, agg.toInt))
        }
        else {
          val province = user.substring(0, 2)
          val city = user.substring(0, 4)
          val agg = "19" + user.substring(6, 8)
          Some(UserInfo(name, idCard, province, city, agg.toInt))
        }
      }
      case _ => None
    }
  }

  def spark() {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val userRDD = sc.textFile("/apps/hive/warehouse/online_data.db/userinfo/part-*")
    val userDF = userRDD.flatMap(parse_User).toDF()
//    userDF.groupBy($"agg").count.show
    userDF.groupBy($"agg").count.orderBy($"agg".desc).show
    userDF.where($"agg" < 1960 and $"agg" > 1945)
//    userDF.where($"agg".equalTo("6048"))
    //    val userDF1 =  userRDD.flatMap(x => {
    //      val arr = x.split("\t")
    //      val idCard = arr(1)
    //      val name = arr(2)
    //      idCard match {
    //        case "-" => None
    //        case _ => Some(UserInfo(name, idCard))
    //      }
    //    }).toDF()

  }

  def main(args: Array[String]): Unit = {
    val user_pattern = """^(\d{15}|\d{18})$""".r
    val idCard = "130324198809110001"
    val name = "tom"
    println(user_pattern.findFirstIn(idCard))
    val aa = idCard match {
      case "-" => None
      case user_pattern(user) => {
        if (user.length == 18) {
          val province = user.substring(0, 2)
          val city = user.substring(0, 4)
          val agg = user.substring(6, 10)
          if (!agg.startsWith("19") && !agg.startsWith("20"))
            None
          else
            Some(UserInfo(name, idCard, province, city, agg))
        }
        else {
          val province = user.substring(0, 2)
          val city = user.substring(0, 4)
          val agg = "19" + user.substring(6, 8)
          Some(UserInfo(name, idCard, province, city, agg))
        }
      }
      case _ => None
    }
    println(aa)
  }
}
