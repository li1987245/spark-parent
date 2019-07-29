package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql demo")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "F:/data/sql")
      .enableHiveSupport() //启动hive支持
      .getOrCreate()
    spark.sql("create table if not exists src (key int,value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
    spark.sql("load data local inpath 'data/kv.txt' into table src")
    spark.sql("select * from src").show()
  }
}
