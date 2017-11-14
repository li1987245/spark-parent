import com.star.util.{IDUtil, RandomValue}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 统计hdfs文件行数
  */
object UserInfoDemo {

  def main(args: Array[String]) {

    System.setProperty("user.name", "hdfs");
    // 设置Spark的序列化方式
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 初始化Spark
    val sparkConf = new SparkConf().setAppName("UserInfoDemo").set("HADOOP_USER_NAME", "hdfs")
    sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.yarn.jar",
      "hdfs://172.168.1.106/hdp/apps/2.5.0.0-1245/spark/spark-hdp-assembly.jar")

    val sc = new SparkContext(sparkConf)
    sc.addJar("/home/jinwei/IdeaProjects/spark-parent/spark-scala/target/spark-scala-1.0-SNAPSHOT.jar")
    //    sc.setLogLevel("WARN")
    sc.setLogLevel("INFO")
    //用户表7百万-》7千万
    val userinfo = sc.textFile("hdfs://creditCluster/apps/hive/warehouse/online_data.db/userinfo")
    //2千万
    //    val acco = sc.textFile("hdfs://creditCluster/apps/hive/warehouse/online_data.db/acco_306")
    val g = new IDUtil()
    for (i <- 0 until 9) {
      val rdd = userinfo.map {
        x => {
          val arr = x.split("\t")
          val rand = Random.nextInt(2)
          if (rand == 0)
            arr(1) = "-"
          else
            arr(1) = g.generate()
          arr(2) = RandomValue.getChineseName()
          arr.mkString("\t")
        }
      }
      rdd.saveAsTextFile("hdfs://creditCluster/hensh/userinfo/" + i)
    }
  }
}