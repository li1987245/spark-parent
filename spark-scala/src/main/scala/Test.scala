import scala.language.implicitConversions
import scala.util.matching.Regex

/**
  * Created by jinwei on 17-12-13.
  */
object Test {


  class IntWritable(_value: Int) {
    def value = _value

    def +(that: IntWritable): IntWritable = {
      new IntWritable(that.value + value)
    }
  }

  def print_str(msg: String): Unit = {

    println(msg)
  }

  implicit def intToStr(i: Int): String = i.toString

  implicit def intToWritable(int: Int) = new IntWritable(int)

  implicit def writableToInt(that: IntWritable) = that.value

  def main(args: Array[String]): Unit = {
    val regex = "http(s?)://(.*?).com.*".r
    val url: String = "http://news.sohu.com/20120612/n345428229.shtml"
    val name = url match {
      case regex(s, n) => n
      case _ => None
    }
    println(name)
    print_str(11)
    val result1 = new IntWritable(10) + 10
    val result2 = 10 + new IntWritable(10)
    val fruit = Fruit((s: String) => println(s))
    fruit.out(fruit.getName)
    for {i <- 1 to 3
         if i % 2 == 0
    }
      yield {
        i + 1
      }
  }
}
