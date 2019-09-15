package structured

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

object StructuredStreamingApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("structured streaming demo")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val df  = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic.*")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val tboxDataSet:Dataset[Row] = df
      .where("topic = my_topic")
      .select(functions.from_json(functions.col("value").cast("string"), df.schema).alias("parsed_value"))
      .select("parsed_value.columnA",
        "parsed_value.columnB",
        "parsed_value.columnC",
        "timestamp")
    val windowtboxDataSet:Dataset[Row] = tboxDataSet
      .withWatermark("timestamp", "5 seconds")
      .groupBy(functions.window(functions.col("timestamp"), "10 minutes", "5 minutes"),
        functions.col("columnA"))
      .count()

  }
}
