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
    userDF.sample(false, 0.01).take(20)
    val userDS = userDF.sample(false, 0.01).as[UserInfo]
    userDS.sample(false, 0.01).filter(x => if (x.name.equals("-")) false else true).take(10).map(x => x.name + "\t" + x.idCard)
    //    Array(朱亚楠	370104197208040041, 郭玉燕	442527196812301342, 李爽	220204196912274560, 吴哲	232101197307310440, 胡志荣	362426198004050052, 陈维他	441623196909173411, 蒋桐生	130603196611170956, 苗军	152322197003171712, 郝永强	410422198410183835, 平蕊	110104198010070428)
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
    sc.broadcast()

  }

  def main(args: Array[String]): Unit = {
    val pattern = "(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?)".r
    val line = "1,2,3,4"
    val result = line match {
      case pattern(a, a1, b, b1, c, c1, d, d1) => Some((a, b, c))
      case _ => None
    }
    println(result)
  }
}
