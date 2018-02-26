package sql

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._


/**
  * Created by jinwei on 18-1-17.
  * hive sample：https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Sampling#LanguageManualSampling-BlockSampling
  * block_sample: TABLESAMPLE (n PERCENT)、block_sample: TABLESAMPLE (nM)、block_sample: TABLESAMPLE (n ROWS)
  * SELECT * FROM my_table TABLESAMPLE (200 ROWS)
  * select * from my_table
  * where rand() <= 0.0001
  * distribute by rand()
  * sort by rand()
  * limit 10000;
  */
object QualityInspection {

  case class Result(col: String, rule: String, flag: Boolean)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    //    val rule:util.HashMap[String,util.Set] = new util.HashMap()
    val rule = scala.collection.mutable.Map.empty[String, scala.collection.mutable.Set[String]]
    rule += ("fullname" -> scala.collection.mutable.Set("not null"))
    rule += ("mobile" -> scala.collection.mutable.Set("not null", "phone"))
    val df = sql("SELECT * FROM online_data.userinfo")
    val schema: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
    df.schema.foreach(st => {
      schema += (st.name -> st.dataType.typeName)
    })
    val count = df.count
    val ratio = 1000.0 / count
    val df_sample = df.sample(withReplacement = false, ratio)
    val result_df = df_sample.flatMap(row => {
      val result = scala.collection.mutable.Set.empty[Result]
      for ((k, v) <- schema) {
        val idx = row.fieldIndex(k)
        if (idx == -1) {
          result += Result(k, "field not match", false)
        }
        else {
          val obj: Any = row.getAs(k)
          //todo 增加判断
          if (obj.isInstanceOf[String]) {
            result += Result(k, "schema", true)
          }
          if (rule.contains(k)) {
            val rules = rule(k)
            for (r <- rules) {
              r match {
                case "not null" => {
                  if (obj == null || obj.equals(""))
                    result += Result(k, "not null", false)
                  else
                    result += Result(k, "not null", true)
                }
                case "phone" => {
                  val tmp = obj.toString
                  result += Result(k, "phone", tmp.matches("\\d+"))
                }
                case _ => None
              }
            }
          }
        }
      }
      result
    })
    val sample_count = df_sample.count
    result_df.where(result_df.col("col").equalTo("mobile")).groupBy("col","rule","flag").count().foreach(row=>{
      val c:Long = row.getAs("count")
      println(row.getAs("col"),row.getAs("rule"),row.getAs("flag"),row.getAs("count"),c*100.0/sample_count)
    })
  }

}
