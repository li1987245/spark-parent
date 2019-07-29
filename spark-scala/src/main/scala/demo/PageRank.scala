package demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object PageRank {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("page rank").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val linksRDD = sc.parallelize(Array(("A",List("E","B","C")),("B",List("A","D","C")),("C",List("F","B","A")),("D",List("E","C")),("E",List("B","C"))))
    var rankRDD = linksRDD.mapValues(_ =>1.0)
    val pageIds = sc.collectionAccumulator[String]("pageIds")
    for (i <- 1 to 10){
      val con = linksRDD.join(rankRDD).flatMap{case(pageId,(links,rank))=>
        pageIds.add(pageId)
        links.map(x=>(x,rank/links.size))
      }.reduceByKey(_+_)
      rankRDD=con.mapValues(v=>0.15+0.85*v)
    }
    rankRDD.map(x=>(x._2,x._1)).sortByKey(ascending = false)
    import scala.collection.JavaConverters._
    val pageIdSet = new mutable.HashSet[String]
    pageIdSet.++(pageIds.value.asScala).foreach(println)
  }
}
