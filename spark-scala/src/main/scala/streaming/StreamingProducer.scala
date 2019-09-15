package streaming

import java.io.PrintWriter
import java.net.ServerSocket
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object StreamingProducer {

  def main(args: Array[String]) {
    socketProduce()
  }

  private def kafkaProduce(): Unit = {
    //kafka节点
    val broker_list = "bi-greenplum-node1:6667,m162p134:6667,m162p135:6667"
    val topic = "test123"
    val isAsync = false
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer")
    val producer = new KafkaProducer[Int, String](props)
    try {
      while (true) {
        val arr = Array("00", "100001", "999999")
        for (code <- arr) {
          val msg:String = Map("response_code"->code,"request_time"->System.currentTimeMillis()).mkString
          val startTime = System.currentTimeMillis()
          if (isAsync) {
            // Send asynchronously
            producer.send(new ProducerRecord(topic, msg), new DemoCallback(startTime, msg))
          } else {
            // Send synchronously
            producer.send(new ProducerRecord(topic, msg))
            System.out.println("Sent message: (" + msg + ")")
          }
        }
        Thread.sleep(30000)
      }
    } catch {
      case ex: Exception =>
        println(ex)
    } finally {
      producer.close()
    }
  }

  /**
    * kafka回调类
    *
    * @param startTime
    * @param message
    */
  class DemoCallback(startTime: Long, message: String) extends Callback {

    override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
      val elapsedTime = System.currentTimeMillis() - startTime
      if (metadata != null) {
        System.out.println(
          "message => (" + message + ") sent to partition(" + metadata.partition() +
            "), " +
            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms")
      } else {
        e.printStackTrace()
      }
    }
  }

  private def socketProduce(): Unit = {
    val random = new Random()
    // 每秒最大活动数
    val MaxEvents = 6
    // 读取可能的名称
    val namesResource =
      this.getClass.getResourceAsStream("/names.csv")
    val names = scala.io.Source.fromInputStream(namesResource)
      .getLines()
      .toList
      .head
      .split(",")
      .toSeq
    // 生成一系列可能的产品
    val products = Seq(
      "iPhone Cover" -> 9.99,
      "Headphones" -> 5.49,
      "Samsung Galaxy Cover" -> 8.95,
      "iPad Cover" -> 7.49
    )

    /** 生成随机产品活动 */
    def generateProductEvents(n: Int) = {
      (1 to n).map { i =>
        val (product, price) =
          products(random.nextInt(products.size))
        val user = random.shuffle(names).head
        (user, product, price)
      }
    }

    // 创建网络生成器
    val listener = new ServerSocket(9999)
    println("Listening on port: 9999")
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " +
            socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream,
            true)
          while (true) {
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvents)
            val productEvents = generateProductEvents(num)
            productEvents.foreach { event =>
              out.write(event.productIterator.mkString(","))
              out.write("\n")
            }
            out.flush()
            println(s"Created $num events...")
          }
          socket.close()
        }
      }.start()
    }
  }
}