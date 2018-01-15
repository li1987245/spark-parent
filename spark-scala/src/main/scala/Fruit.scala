/**
  * Created by jinwei on 18-1-15.
  */
trait Fruit {
  //s为任意参数，返回值为string的函数,高阶函数（Higher-Order Function）就是操作其他函数的函数
  def out(s: => String): Unit

  def getName:String = "Fruit"
}


object Fruit {
  def apply(f: String => Unit): Fruit = new Fruit {
    //s为任意参数，返回值为string的函数,f为参数为string，返回值为空的高阶函数
    def out(s: => String): Unit = f(s)
  }
}