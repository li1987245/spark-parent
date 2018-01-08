import scala.util.matching.Regex

/**
  * Created by jinwei on 17-12-13.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val regex = "http(s?)://(.*?).com.*".r
    val url: String = "http://news.sohu.com/20120612/n345428229.shtml"
    val name = url match {
      case regex(s, n) => n
      case _ => None
    }
    println(name)
  }
}
