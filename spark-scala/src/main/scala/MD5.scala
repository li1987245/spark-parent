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
    val nameSet = List("朱亚楠", "郭玉燕", "李爽", "吴哲", "胡志荣", "陈维他", "蒋桐生", "苗军", "郝永强", "平蕊")
    val idCardSet = List("370104197208040041","442527196812301342","220204196912274560","232101197307310440","362426198004050052","441623196909173411","130603196611170956","152322197003171712","410422198410183835","110104198010070428")
    for(i<-0 until nameSet.size)
      {
        println(hash(nameSet.apply(i)+idCardSet.apply(i)+"MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8"))
      }
    val md5Set = Set("825edfef80fa4d1a88d549f8caf6df0d","9e650b657ecd936e80f6bc3c7910e620","cf22c7817a69e4e89ff526180bf9d048","505402bfbcf263a0c9ec7afa43fd43a1","2f78f2398e6d57c596989a9c4a95409f","936391a782636332e7ae03805884a36a","3589a3baa0122d440aac82563d1cc71f","e082106aabf8c5ddc964881322eb56a2","de15d3560c078dc458e036ff79bc1e8d","69ef4c8432eab9038b88d3961067bb1e")
  }
}
