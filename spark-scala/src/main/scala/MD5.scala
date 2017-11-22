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
    val seqSet = List.range(1, 1000)
    var tmp: Set[String] = Set()
    val x = ""
    seqSet.foreach(y => {
      var s = ""
      if (y < 10)
        s = x.toString + "00" + y
      else if (y < 100 && y > 9)
        s = x.toString + "0" + y
      else
        s = x.toString + y
      tmp += s.toString
    })
    tmp.foreach(println)
  }
}
