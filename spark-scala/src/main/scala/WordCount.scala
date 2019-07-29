import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
  * standalone方式执行
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 3, 1)
    val mapping = Map[Int, Int](1 -> 0, 2 -> 0, 3 -> 0)
    var br: Broadcast[Map[Int, Int]] = sc.broadcast(mapping)
    val updateProd = new Thread(new Runnable {
      override def run(): Unit = {
        val mapping = Map[Int, Int](1 -> 1, 2 -> 1, 3 -> 1)
        br = sc.broadcast(mapping)
      }
    }
    )
    updateProd.start()
    val distData = sc.parallelize(data)
    val maps = sc.parallelize(br.value.toSeq)
    import scala.reflect.runtime.universe._
    maps.collect().toMap.foreach(x=>println(x.getClass))
    println("------------------")
    val tmp = distData.map((_, 1)).reduceByKey(_ + _).join(maps).cache()
    tmp.sortBy(x=>x._1).foreach(println)
    println("------------------")
    tmp.flatMap(x => Array(x._2._1,x._2._2)).sortBy(x=>x,ascending = true).foreach(println)
    tmp.unpersist()
  }
}
