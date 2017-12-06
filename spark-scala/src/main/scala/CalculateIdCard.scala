import java.util

import com.star.util.{Constans, IDUtil, IdcardValidator, StringUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//引入scala和java转换
import scala.collection.JavaConversions._

/**
  * cache和persist的区别：cache只有一个默认的缓存级别MEMORY_ONLY ，而persist可以根据情况设置其它的缓存级别。
  * useDisk：使用硬盘（外存）
  * useMemory：使用内存
  * useOffHeap：使用堆外内存，这是Java虚拟机里面的概念，堆外内存意味着把内存对象分配在Java虚拟机的堆以外的内存，这些内存直接受操作系统管理（而不是虚拟机）。这样做的结果就是能保持一个较小的堆，以减少垃圾收集对应用的影响。
  * deserialized：反序列化，其逆过程序列化（Serialization）是java提供的一种机制，将对象表示成一连串的字节；而反序列化就表示将字节恢复为对象的过程。序列化是对象永久化的一种机制，可以将对象及其属性保存起来，并能在反序列化后直接恢复这个对象
  * replication：备份数（在多个节点上备份）
  * Created by jinwei on 17-11-14.
  */
object CalculateIdCard {

  def sparkDemo(): Unit = {
    val nameSet = List("刘玲萍", "陈为", "杨云电", "李红梅", "李定国", "蒋德明", "周良君", "施后力", "李杨", "胡碧新", "王永泉", "易玲", "罗于红", "黄红", "林丹文", "刘伟", "李云峰", "昌宏顺", "李国强", "李春华", "李翔", "王潇颖", "林亦根", "黄玉顺", "王萍萍", "王述科", "陈玉生", "刘伟", "李长春", "蒲丽娟", "罗跃鸣", "郑跃权", "李小玲", "曾祥品", "陈青群", "葛剑平", "李涛", "熊鑫", "李红霞", "李习中", "顾振德", "张华", "许志琼", "童永", "黄明")
    val md5Set = Set("b56f18ba60f87e57e30889c110f4a428", "05709bd08e8934e39dae42d87b2926af", "1f0a606673e19375675693a78e4d6fc0", "b7031f87bd3aa17e5d86ba532e7304c2", "71381448d2c1df9f976c58c064744541", "bc15ddf4d4d557d438b4c8da072caec3", "3c1cc29fa3859acab53af7dea95f8e7b", "4e9918f7b1b0807120409baa26301ba9", "db46b25d10b4d4ceb4843852ad31ddaa", "5789e561da73469d63e068702301c0c8", "5db945db770ae38daf73fc73ac8166be", "9e2d479b5bb61c508f412c63d211e191", "ed8b337b6b43c35ce9c9a9598f3f9000", "d7460e8a1b376c727866c1feee322fb7", "91469c701342c0bb23836cae3a609230", "d9c855d19c8c3a56492a445fe4f87ab3", "76a5d6f6bb40877b5dfa289699b4464f", "cf3fd21620f706364c2491a8a140a9a7", "4f1dd61af2c4b9c27c7b1c6c46b559d2", "f9081a3d2dc0ba51e61dc1ab0ef69b55", "c00251c33313121a6247461e6af33e41", "588e3a513c1be36f5c302170dd9cd59b", "2f2d9e63cfcbc8552e025303a5db08fd", "b70d9ca3052bb7bc9c50f7417f71fe0a", "2ab6715f2421737191d55dd4e97dff74", "e0647b5cf1d047cd60cbe6d1ed24199c", "cfd0a19be0efd0278f98b7e865b9245b", "d9c855d19c8c3a56492a445fe4f87ab3", "5b545938b9382cd2493310b9c0d7b7bc", "cb0824ba103f0b2697e77a1a1693d0d6", "860e2c31ee32909f970a5a476437d544", "c0f6e8f1d481666af2869002c99900e8", "a1df953480cee8930a968a61bdd2b776", "b369ed684db653af0b64f9a370279cb7", "d69d6d5208ec5d24332bb70e9ab80661", "321de02ea181020318c8215f6789b1ed", "944451c0cd73558e71b62d35b5c2b80d", "0119eea6fe28f0effea1d2f0c02bff0c", "f23a7563d29c153f737c7feac9d05300", "0289d22d7898e8aafd6cf4d9921b715a", "1ce83026b82382a2e893264ca831754f", "7935981bc9e8d61cdd345df729a9927f", "081f62ec383ac0bed813a47cc25d0cbd", "1da9617d68dbe5605a6dbf304328f8d6", "0de431916fb67a500e5006850a2f6097")
    val salt = "MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8"
    //身份证1-6位
    val areaCodeOldSet: Set[Int] = IdcardValidator.getAreaCodeSet.map(_.toInt).toSet
    val areaCodeAllSet: Set[Int] = Constans.areaCodeAll.split(",").map(_.toInt).toSet
    val areaCodeSet = areaCodeAllSet.diff(areaCodeOldSet).toArray
    //身份证7-10 1947-1985:Set[Integer]
    //    val yearSet = List(1972, 1968, 1969, 1973, 1980, 1966, 1970, 1984)
    val yearSet = List.range(1942, 1986)
    //月份11-12 1-12
    val monthSet = List.range(1, 13)
    //日期13-14 1-31
    val daySet = List.range(1, 32)
    //顺序15-17（派出所和17位性别顺序）
    val seqSet = List.range(1, 1000)
    //    val seqSet = List.range(200, 400)
    //    val seqSet = List.range(400, 600)
    //    val seqSet = List.range(600, 800)
    //    val seqSet = List.range(800, 1000)
    //18
    val signSet = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "X", "x")

    val sparkConf = new SparkConf().setAppName("cal idCard")
    //设置spark.default.parallelism参数，防止spark计算出来的partition非常巨大
    //    sparkConf.set("spark.default.parallelism", "40")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    sparkConf.setMaster("yarn-client")
    //    sparkConf.set("spark.yarn.jar",
    //      "hdfs://172.168.1.106/hdp/apps/2.5.0.0-1245/spark/spark-hdp-assembly.jar")
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")
    //设置hadoop属性
    sc.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy",
      "NEVER"
    )
    sc.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable",
      "true"
    )
    val areaRDD: RDD[String] = sc.makeRDD(areaCodeSet).mapPartitions(x => {
      var result = List[String]()
      while (x.hasNext) {
        result = result.::(x.next().toString)
      }
      result.iterator
    })

    val yearRDD: RDD[String] = sc.makeRDD(yearSet).map(x => x.toString)
    val monthRDD: RDD[String] = sc.makeRDD(monthSet).map(x => (if (x < 10) "0" + x else x).toString)
    val dayRDD: RDD[String] = sc.makeRDD(daySet).map(x => (if (x < 10) "0" + x else x).toString)
    val seqRDD: RDD[String] = sc.makeRDD(seqSet).map(x => if (x < 10)
      "00" + x
    else if (x < 100 && x > 9) "0" + x else x.toString)
    val signRDD: RDD[String] = sc.makeRDD(signSet).map(x => x.toString)

    //    val idCardRDD = areaRDD.cartesian(yearRDD).map(x => {
    //      x._1 + x._2
    //    }).cartesian(monthRDD).map(x => {
    //      x._1 + x._2
    //    }).cartesian(dayRDD).map(x => {
    //      x._1 + x._2
    //    }).cartesian(seqRDD).map(x => {
    //      x._1 + x._2
    //    }).cartesian(signRDD).map(x => {
    //      x._1 + x._2
    //    })
    val nameRDD: RDD[String] = sc.makeRDD(nameSet)
    //    val rdd = nameRDD.cartesian(idCardRDD)
    //    rdd.filter(x => {
    //      val md5 = MD5.hash(x._1 + x._2 + salt)
    //      if (md5Set.contains(md5))
    //        true
    //      else
    //        false
    //    }).saveAsTextFile("/tmp/idCard")

    val rdd0 = areaRDD.flatMap(x => {
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
        else if (y < 100 && y > 9)
          s = x.toString + "0" + y
        else
          s = x.toString + y
        val sign = IDUtil.calcSignNum(s.toCharArray)
        s = s + sign
        tmp += s.toString
      })
      tmp
    })
    //    val rdd = rdd0.repartition(1200)
    //释放缓存
    //    rdd0.unpersist()
    //    rdd.cache()
    //    rdd.count()
    rdd0.filter(x => {
      var b = false
      //姓名
      nameSet.foreach(y => {
        val md5 = StringUtil.md5(y + x + salt)
        if (md5Set.contains(md5)) {
          println(x)
          b = true
          //return导致java.io.NotSerializableException: java.lang.Object
          //          return
        }

      })
      b
    }).saveAsTextFile("/tmp/idCard8")

  }

  def scalaDemo(): Unit = {
    val nameSet = List("王潇颖", "蒲丽娟")
    val md5Set = Set("b56f18ba60f87e57e30889c110f4a428", "05709bd08e8934e39dae42d87b2926af", "1f0a606673e19375675693a78e4d6fc0", "b7031f87bd3aa17e5d86ba532e7304c2", "71381448d2c1df9f976c58c064744541", "bc15ddf4d4d557d438b4c8da072caec3", "3c1cc29fa3859acab53af7dea95f8e7b", "4e9918f7b1b0807120409baa26301ba9", "db46b25d10b4d4ceb4843852ad31ddaa", "5789e561da73469d63e068702301c0c8", "5db945db770ae38daf73fc73ac8166be", "9e2d479b5bb61c508f412c63d211e191", "ed8b337b6b43c35ce9c9a9598f3f9000", "d7460e8a1b376c727866c1feee322fb7", "91469c701342c0bb23836cae3a609230", "d9c855d19c8c3a56492a445fe4f87ab3", "76a5d6f6bb40877b5dfa289699b4464f", "cf3fd21620f706364c2491a8a140a9a7", "4f1dd61af2c4b9c27c7b1c6c46b559d2", "f9081a3d2dc0ba51e61dc1ab0ef69b55", "c00251c33313121a6247461e6af33e41", "588e3a513c1be36f5c302170dd9cd59b", "2f2d9e63cfcbc8552e025303a5db08fd", "b70d9ca3052bb7bc9c50f7417f71fe0a", "2ab6715f2421737191d55dd4e97dff74", "e0647b5cf1d047cd60cbe6d1ed24199c", "cfd0a19be0efd0278f98b7e865b9245b", "d9c855d19c8c3a56492a445fe4f87ab3", "5b545938b9382cd2493310b9c0d7b7bc", "cb0824ba103f0b2697e77a1a1693d0d6", "860e2c31ee32909f970a5a476437d544", "c0f6e8f1d481666af2869002c99900e8", "a1df953480cee8930a968a61bdd2b776", "b369ed684db653af0b64f9a370279cb7", "d69d6d5208ec5d24332bb70e9ab80661", "321de02ea181020318c8215f6789b1ed", "944451c0cd73558e71b62d35b5c2b80d", "0119eea6fe28f0effea1d2f0c02bff0c", "f23a7563d29c153f737c7feac9d05300", "0289d22d7898e8aafd6cf4d9921b715a", "1ce83026b82382a2e893264ca831754f", "7935981bc9e8d61cdd345df729a9927f", "081f62ec383ac0bed813a47cc25d0cbd", "1da9617d68dbe5605a6dbf304328f8d6", "0de431916fb67a500e5006850a2f6097")
    val salt = "MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8"
    //身份证1-6位
    val areaCodeSet = IdcardValidator.getAreaCodeSet
    //身份证7-10 1980-2000
    val yearSet = List(1972, 1980)
    //月份11-12 1-12
    val monthSet = List(10, 11)
    //日期13-14 1-31
    val daySet = List(25, 9)
    //顺序15-17（派出所和17位性别顺序）
    val seqSet = List(8, 152)
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
        else if (y < 100 && y > 9)
          s = x.toString + "0" + y
        else
          s = x.toString + y
        val sign = IDUtil.calcSignNum(s.toCharArray)
        s = s + sign
        tmp += s.toString
      })
      tmp
    }).foreach(x => {
      //姓名
      nameSet.foreach(y => {
        val md5 = MD5.hash(y + x + salt)
        if (md5Set.contains(md5.toLowerCase))
          println(x)
      })

    })
  }


  def main(args: Array[String]): Unit = {
//    sparkDemo()
    //    scalaDemo()
    //    val nameSet = List("刘玲萍", "陈为", "杨云电", "李红梅", "李定国", "蒋德明", "周良君", "施后力", "李杨", "胡碧新", "王永泉", "易玲", "罗于红", "黄红", "林丹文", "刘伟", "李云峰", "昌宏顺", "李国强", "李春华", "李翔", "王潇颖", "林亦根", "黄玉顺", "王萍萍", "王述科", "陈玉生", "刘伟", "李长春", "蒲丽娟", "罗跃鸣", "郑跃权", "李小玲", "曾祥品", "陈青群", "葛剑平", "李涛", "熊鑫", "李红霞", "李习中", "顾振德", "张华", "许志琼", "童永", "黄明")
    //    val md5Set = Set("b56f18ba60f87e57e30889c110f4a428", "05709bd08e8934e39dae42d87b2926af", "1f0a606673e19375675693a78e4d6fc0", "b7031f87bd3aa17e5d86ba532e7304c2", "71381448d2c1df9f976c58c064744541", "bc15ddf4d4d557d438b4c8da072caec3", "3c1cc29fa3859acab53af7dea95f8e7b", "4e9918f7b1b0807120409baa26301ba9", "db46b25d10b4d4ceb4843852ad31ddaa", "5789e561da73469d63e068702301c0c8", "5db945db770ae38daf73fc73ac8166be", "9e2d479b5bb61c508f412c63d211e191", "ed8b337b6b43c35ce9c9a9598f3f9000", "d7460e8a1b376c727866c1feee322fb7", "91469c701342c0bb23836cae3a609230", "d9c855d19c8c3a56492a445fe4f87ab3", "76a5d6f6bb40877b5dfa289699b4464f", "cf3fd21620f706364c2491a8a140a9a7", "4f1dd61af2c4b9c27c7b1c6c46b559d2", "f9081a3d2dc0ba51e61dc1ab0ef69b55", "c00251c33313121a6247461e6af33e41", "588e3a513c1be36f5c302170dd9cd59b", "2f2d9e63cfcbc8552e025303a5db08fd", "b70d9ca3052bb7bc9c50f7417f71fe0a", "2ab6715f2421737191d55dd4e97dff74", "e0647b5cf1d047cd60cbe6d1ed24199c", "cfd0a19be0efd0278f98b7e865b9245b", "d9c855d19c8c3a56492a445fe4f87ab3", "5b545938b9382cd2493310b9c0d7b7bc", "cb0824ba103f0b2697e77a1a1693d0d6", "860e2c31ee32909f970a5a476437d544", "c0f6e8f1d481666af2869002c99900e8", "a1df953480cee8930a968a61bdd2b776", "b369ed684db653af0b64f9a370279cb7", "d69d6d5208ec5d24332bb70e9ab80661", "321de02ea181020318c8215f6789b1ed", "944451c0cd73558e71b62d35b5c2b80d", "0119eea6fe28f0effea1d2f0c02bff0c", "f23a7563d29c153f737c7feac9d05300", "0289d22d7898e8aafd6cf4d9921b715a", "1ce83026b82382a2e893264ca831754f", "7935981bc9e8d61cdd345df729a9927f", "081f62ec383ac0bed813a47cc25d0cbd", "1da9617d68dbe5605a6dbf304328f8d6", "0de431916fb67a500e5006850a2f6097")
    //    val idCard = "510183198011090084"
    //    val salt = "MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8"
    //    nameSet.filter(y => {
    //      val md5 = MD5.hash(y + idCard + salt)
    //      if (md5Set.contains(md5))
    //        true
    //      else
    //        false
    //    }).foreach(println)
        val areaCodeOldSet: Set[Int] = IdcardValidator.getAreaCodeSet.map(_.toInt).toSet
        val areaCodeAllSet: Set[Int] = Constans.areaCodeAll.split(",").map(_.toInt).toSet
    println(areaCodeAllSet.size)
        val areaCodeSet = areaCodeAllSet.diff(areaCodeOldSet)
        println(areaCodeSet.size)
  }

}
