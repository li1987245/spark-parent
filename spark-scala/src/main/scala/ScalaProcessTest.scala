import scala.sys.process._

/**
  * Created by jinwei on 18-1-15.
  */
object ScalaProcessTest {
  def main(args: Array[String]): Unit = {
    val out = new StringBuilder
    val err = new StringBuilder

    val logger = ProcessLogger(
      (o: String) => println(o),
      (e: String) => err.append(e))
    "sh /home/jinwei/test.sh" ! logger
    println("err:\t"+err)
  }
}
