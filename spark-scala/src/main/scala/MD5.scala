/**
  * Created by jinwei on 17-11-14.
  */
object MD5 {
  def hash(s: String) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }

  def main(args: Array[String]): Unit = {
    var list:List[Int] = List()
    list.::(1)
    println(list)
  }
}
