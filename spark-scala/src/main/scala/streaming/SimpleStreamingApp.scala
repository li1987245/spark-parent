package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 用Scala写的一个简单的Spark Streaming应用
  */
object SimpleStreamingApp {
  def main(args: Array[String]) {
    val ssc = new StreamingContext("yarn",
      "First Streaming App", Seconds(10))
    val stream = ssc.socketTextStream("localhost", 9999)
    // 简单地打印每一批的前几个元素
    // 批量运行
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}