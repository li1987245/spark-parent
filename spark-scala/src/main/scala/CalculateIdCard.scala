import java.util

import org.apache.spark.{SparkConf, SparkContext}
//引入scala和java转换
import scala.collection.JavaConversions._

/**
  * Created by jinwei on 17-11-14.
  */
object CalculateIdCard {

  def sparkDemo(): Unit = {
    val nameSet = List("刘玲萍", "陈为", "杨云电", "李红梅", "李定国", "蒋德明", "周良君", "施后力", "李杨", "胡碧新", "王永泉", "易玲", "罗于红", "黄红", "林丹文", "刘伟", "李云峰", "昌宏顺", "李国强", "李春华", "李翔", "王潇颖", "林亦根", "黄玉顺", "王萍萍", "王述科", "陈玉生", "刘伟", "李长春", "蒲丽娟", "罗跃鸣", "郑跃权", "李小玲", "曾祥品", "陈青群", "葛剑平", "李涛", "熊鑫", "李红霞", "李习中", "顾振德", "张华", "许志琼", "童永", "黄明")
    val md5Set = Set("b56f18ba60f87e57e30889c110f4a428", "05709bd08e8934e39dae42d87b2926af", "1f0a606673e19375675693a78e4d6fc0", "b7031f87bd3aa17e5d86ba532e7304c2", "71381448d2c1df9f976c58c064744541", "bc15ddf4d4d557d438b4c8da072caec3", "3c1cc29fa3859acab53af7dea95f8e7b", "4e9918f7b1b0807120409baa26301ba9", "db46b25d10b4d4ceb4843852ad31ddaa", "5789e561da73469d63e068702301c0c8", "5db945db770ae38daf73fc73ac8166be", "9e2d479b5bb61c508f412c63d211e191", "ed8b337b6b43c35ce9c9a9598f3f9000", "d7460e8a1b376c727866c1feee322fb7", "91469c701342c0bb23836cae3a609230", "d9c855d19c8c3a56492a445fe4f87ab3", "76a5d6f6bb40877b5dfa289699b4464f", "cf3fd21620f706364c2491a8a140a9a7", "4f1dd61af2c4b9c27c7b1c6c46b559d2", "f9081a3d2dc0ba51e61dc1ab0ef69b55", "c00251c33313121a6247461e6af33e41", "588e3a513c1be36f5c302170dd9cd59b", "2f2d9e63cfcbc8552e025303a5db08fd", "b70d9ca3052bb7bc9c50f7417f71fe0a", "2ab6715f2421737191d55dd4e97dff74", "e0647b5cf1d047cd60cbe6d1ed24199c", "cfd0a19be0efd0278f98b7e865b9245b", "d9c855d19c8c3a56492a445fe4f87ab3", "5b545938b9382cd2493310b9c0d7b7bc", "cb0824ba103f0b2697e77a1a1693d0d6", "860e2c31ee32909f970a5a476437d544", "c0f6e8f1d481666af2869002c99900e8", "a1df953480cee8930a968a61bdd2b776", "b369ed684db653af0b64f9a370279cb7", "d69d6d5208ec5d24332bb70e9ab80661", "321de02ea181020318c8215f6789b1ed", "944451c0cd73558e71b62d35b5c2b80d", "0119eea6fe28f0effea1d2f0c02bff0c", "f23a7563d29c153f737c7feac9d05300", "0289d22d7898e8aafd6cf4d9921b715a", "1ce83026b82382a2e893264ca831754f", "7935981bc9e8d61cdd345df729a9927f", "081f62ec383ac0bed813a47cc25d0cbd", "1da9617d68dbe5605a6dbf304328f8d6", "0de431916fb67a500e5006850a2f6097")
    val salt = "MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8"
    //身份证1-6位
    val areaCodeSet = IdcardValidator.getAreaCodeSet
    //身份证7-10 1980-2000
    val yearSet = List.range(1970, 2000)
    //月份11-12 1-12
    val monthSet = List.range(1, 13)
    //日期13-14 1-31
    val daySet = List.range(1, 32)
    //顺序15-17（派出所和17位性别顺序）
    val seqSet = List.range(1, 1000)
    //18
    val signSet = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "X")

    val sparkConf = new SparkConf().setAppName("cal idCard")
    sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.yarn.jars",
      "hdfs://localhost:8020/user/spark2/jars/*.jar")
    val sc = SparkContext.getOrCreate(sparkConf)

    val areaRDD = sc.makeRDD(areaCodeSet)

    val rdd = areaRDD.flatMap(x => {
      //年份
      var tmp: Set[String] = Set()
      yearSet.foreach(y => {
        tmp += (x.toString + y).toString
      })
      tmp
    }).flatMap(x => {
      //月份
      var tmp: Set[String] = Set()
      monthSet.foreach(y => {
        tmp += (if (y < 10) x.toString + "0" + y else x.toString + y).toString
      })
      tmp
    }).flatMap(x => {
      //日期
      var tmp: Set[String] = Set()
      daySet.foreach(y => {
        tmp += (if (y < 10) x.toString + "0" + y else x.toString + y).toString
      })
      tmp
    }).flatMap(x => {
      //顺序
      var tmp: Set[String] = Set()
      seqSet.foreach(y => {
        var s = ""
        if (y < 10)
          s = x.toString + "00" + y
        else if (y < 100 && y > 9) x.toString + "0" + y else x.toString + y
        tmp += s.toString
      })
      tmp
    }).flatMap(x => {
      //sign
      var tmp: Set[String] = Set()
      signSet.foreach(y => {
        tmp += (x.toString + y).toString
      })
      tmp
    })

    rdd.filter(x => {
      var b = false
      //姓名
      nameSet.foreach(y => {
        val md5 = MD5.hash(y + x + salt)
        if (md5Set.contains(md5)) {
          b = true
          //return导致java.io.NotSerializableException: java.lang.Object
//          return
        }

      })
      b
    }).saveAsTextFile("/tmp/idCard")

  }

  def scalaDemo(): Unit = {
    val nameSet = List("刘玲萍", "陈为", "杨云电", "李红梅", "李定国", "蒋德明", "周良君", "施后力", "李杨", "胡碧新", "王永泉", "易玲", "罗于红", "黄红", "林丹文", "刘伟", "李云峰", "昌宏顺", "李国强", "李春华", "李翔", "王潇颖", "林亦根", "黄玉顺", "王萍萍", "王述科", "陈玉生", "刘伟", "李长春", "蒲丽娟", "罗跃鸣", "郑跃权", "李小玲", "曾祥品", "陈青群", "葛剑平", "李涛", "熊鑫", "李红霞", "李习中", "顾振德", "张华", "许志琼", "童永", "黄明")
    val md5Set = Set("b56f18ba60f87e57e30889c110f4a428", "05709bd08e8934e39dae42d87b2926af", "1f0a606673e19375675693a78e4d6fc0", "b7031f87bd3aa17e5d86ba532e7304c2", "71381448d2c1df9f976c58c064744541", "bc15ddf4d4d557d438b4c8da072caec3", "3c1cc29fa3859acab53af7dea95f8e7b", "4e9918f7b1b0807120409baa26301ba9", "db46b25d10b4d4ceb4843852ad31ddaa", "5789e561da73469d63e068702301c0c8", "5db945db770ae38daf73fc73ac8166be", "9e2d479b5bb61c508f412c63d211e191", "ed8b337b6b43c35ce9c9a9598f3f9000", "d7460e8a1b376c727866c1feee322fb7", "91469c701342c0bb23836cae3a609230", "d9c855d19c8c3a56492a445fe4f87ab3", "76a5d6f6bb40877b5dfa289699b4464f", "cf3fd21620f706364c2491a8a140a9a7", "4f1dd61af2c4b9c27c7b1c6c46b559d2", "f9081a3d2dc0ba51e61dc1ab0ef69b55", "c00251c33313121a6247461e6af33e41", "588e3a513c1be36f5c302170dd9cd59b", "2f2d9e63cfcbc8552e025303a5db08fd", "b70d9ca3052bb7bc9c50f7417f71fe0a", "2ab6715f2421737191d55dd4e97dff74", "e0647b5cf1d047cd60cbe6d1ed24199c", "cfd0a19be0efd0278f98b7e865b9245b", "d9c855d19c8c3a56492a445fe4f87ab3", "5b545938b9382cd2493310b9c0d7b7bc", "cb0824ba103f0b2697e77a1a1693d0d6", "860e2c31ee32909f970a5a476437d544", "c0f6e8f1d481666af2869002c99900e8", "a1df953480cee8930a968a61bdd2b776", "b369ed684db653af0b64f9a370279cb7", "d69d6d5208ec5d24332bb70e9ab80661", "321de02ea181020318c8215f6789b1ed", "944451c0cd73558e71b62d35b5c2b80d", "0119eea6fe28f0effea1d2f0c02bff0c", "f23a7563d29c153f737c7feac9d05300", "0289d22d7898e8aafd6cf4d9921b715a", "1ce83026b82382a2e893264ca831754f", "7935981bc9e8d61cdd345df729a9927f", "081f62ec383ac0bed813a47cc25d0cbd", "1da9617d68dbe5605a6dbf304328f8d6", "0de431916fb67a500e5006850a2f6097")
    val salt = "MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8"
    //身份证1-6位
    val areaCodeSet = IdcardValidator.getAreaCodeSet
    //身份证7-10 1980-2000
    val yearSet = List.range(1980, 2000)
    //月份11-12 1-12
    val monthSet = List.range(1, 13)
    //日期13-14 1-31
    val daySet = List.range(1, 32)
    //顺序15-17（派出所和17位性别顺序）
    val seqSet = List.range(1, 1000)
    //18
    val signSet = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "X")
    //区域
    areaCodeSet.flatMap(x => {
      //年份
      var tmp: Set[String] = Set()
      yearSet.foreach(y => {
        tmp += (x.toString + y)
      })
      tmp
    }).flatMap(x => {
      //月份
      var tmp: Set[String] = Set()
      monthSet.foreach(y => {
        tmp += (if (y < 10) x.toString + "0" + y else x.toString + y)
      })
      tmp
    }).flatMap(x => {
      //日期
      var tmp: Set[String] = Set()
      daySet.foreach(y => {
        tmp += (if (y < 10) x.toString + "0" + y else x.toString + y)
      })
      tmp
    }).flatMap(x => {
      //顺序
      var tmp: Set[String] = Set()
      seqSet.foreach(y => {
        var s = ""
        if (y < 10)
          s = x.toString + "00" + y
        else if (y < 100 && y > 9) x.toString + "0" + y else x.toString + y
        tmp += s
      })
      tmp
    }).flatMap(x => {
      //sign
      var tmp: Set[String] = Set()
      signSet.foreach(y => {
        tmp += (x.toString + y)
      })
      tmp
    }).foreach(x => {
      //姓名
      nameSet.foreach(y => {
        val md5 = MD5.hash(y + x + salt)
        if (md5Set.contains(md5))
          println(x)
      })

    })
  }


  def main(args: Array[String]): Unit = {
    sparkDemo()
  }

}
