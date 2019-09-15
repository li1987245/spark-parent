package streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.SizeEstimator

/**
  * 用Scala写的一个简单的Spark Streaming应用
  * nc -lk 9999
  * spark-shell 方式: val ssc=new StreamingContext(sc,Seconds(10))
  */
object SimpleStreamingApp {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("spark.debug.maxToStringFields", "1000")
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
      .setAppName("First Streaming App")
    //    conf.registerKryoClasses(Array(classOf[MyClass1]))
    val ssc = new StreamingContext(conf, Seconds(10))
    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport() //添加对HIVE的支持，否则无法访问hive库
      .getOrCreate()
    import spark.implicits._
    val df = readMysql(spark)
    df.cache()
    val map = df.collect
    val map1 = map.map(row => {
      row.getValuesMap[String](row.schema.fieldNames)
    })
    println(SizeEstimator.estimate(map))
    println(SizeEstimator.estimate(map1))
    val bc = spark.sparkContext.broadcast(map)
    println(SizeEstimator.estimate(bc))
    Thread.sleep(100000)
    //    _kafkaStream10(ssc)
    //    ssc.start()
    //    ssc.awaitTermination()
  }

  private def _kafkaStream10(ssc: StreamingContext) = {
    //kafka节点
    val BROKER_LIST = "bi-greenplum-node1:6667,m162p134:6667,m162p135:6667"
    /*
        参数说明
        AUTO_OFFSET_RESET_CONFIG
            earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
            latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
            none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         */
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BROKER_LIST,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka_group",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val topics = Array("test10")
    //创建数据流
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //处理流数据
    stream.foreachRDD { rdd =>
      //手动维护偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>
        iter.foreach(line => {
          println(line.value())
        })
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      // 在输出（outputs）完成一段时间之后，将偏移量同步更新到kafka
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }

  private def kafkaStream10(ssc: StreamingContext) = {
    //kafka节点
    val BROKER_LIST = "bi-greenplum-node1:6667,m162p134:6667,m162p135:6667"
    val ZK_SERVERS = "hdc-data4:2181,hdc-data5:2181,hdc-data6:2181"
    val GROUP_ID = "kafka_group" //消费者组

    val topics = Array("test123") //待消费topic

    /*
    参数说明
    AUTO_OFFSET_RESET_CONFIG
        earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BROKER_LIST,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> GROUP_ID,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    //采用zookeeper手动维护偏移量
    val zkManager = new KafkaOffsetZKManager(ZK_SERVERS)
    val fromOffsets = zkManager.getFromOffset(topics, GROUP_ID)
    //创建数据流
    var stream: InputDStream[ConsumerRecord[String, String]] = null
    if (fromOffsets.nonEmpty) {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams, fromOffsets)
      )
    } else {
      stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      println("第一次消费 Topic:" + topics)
    }

    //处理流数据
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val rs = rdd.map(record => (record.offset(), record.partition(), record.value())).collect()
      for (item <- rs) println(item)
      // 处理数据存储到HDFS或Hbase等
      // 存储代码（略）
      // 处理完数据保存/更新偏移量
      zkManager.storeOffsets(offsetRanges, GROUP_ID)
    }
  }

  private def socketStream(ssc: StreamingContext) = {
    val stream = ssc.socketTextStream("localhost", 9999)
    stream.print()
  }

  private def readMysql(spark: SparkSession): DataFrame = {

    //读取mysql中数据，返回数据类型为DataSet
    val df = spark.read
      .format("jdbc")
      .options(Map("url" ->
        //配置mysql连接参数，包括mysql ip 端口  数据库名称 登录名和密码
        "jdbc:mysql://192.168.162.192:3306/field_monitor?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull",
        "user" -> "field_monitor",
        "password" -> "field_monitor-20190724",
        //定义驱动程序
        "driver" -> "com.mysql.jdbc.Driver",
        //编写sql  在mysql中执行该sql并返回数据
        "dbtable" -> "(select * from rule_config where rule_status=1 and rule_type='codeRule') as rules"))
      .load()
    df
  }

  private def writeMysql(df: DataFrame): Unit = {

    val prop = new java.util.Properties()
    prop.put("user", "username")
    prop.put("password", "yourpassword")
    val url = "jdbc:mysql://host:3306/db_name"
    //df是一个dataframe,也就是你要写入的数据
    df.write.mode(SaveMode.Append).jdbc(url, "table_name", prop)
  }
}