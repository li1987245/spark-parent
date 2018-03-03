import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import util.control.Breaks._
import scala.collection.mutable

object IdGraph {
  case class Label(key:String, vertex:String, leaves:mutable.Set[String], isActivation:Boolean)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn").setAppName("IdGraph")
    val sc = new SparkContext(conf)
    var rdd = sc.textFile("file:///tmp/id_graph").map(line=>{
      val arr = line.split(",")
      val vertex = arr.min
      val leaves = mutable.Set[String]()
      Label(vertex,vertex,leaves.++(arr),true)
    })

    for(i<-0 to 100){
      val accum = sc.longAccumulator("accumulator")
      rdd = rdd.flatMap(label=>{
        val tmp = mutable.Set[Label]()
        tmp += label
        val vertex = label.vertex
        if(label.isActivation){
          label.leaves.foreach(
            tmp += Label(_,vertex,null,true)
          )
        }
        println(tmp)
        tmp
      }).groupBy(label=>{
        label.key
      }).flatMap(x=>{
        val (key,values) = x
        var vertex = values.head.vertex
        var isActivation = false
        val leaves = mutable.Set[String]()
        values.foreach(label=>{
          val _vertex = label.vertex
          if(_vertex.compareTo(vertex)<0){
            vertex = _vertex
            isActivation = true
            accum.add(1)
          }
          if(label.leaves != null)
            leaves ++= label.leaves
        })
        if(leaves.nonEmpty){
          leaves += key
          Some(Label(vertex,vertex,leaves,isActivation))
        }
        else
          None
      })
      rdd.count
      println(accum.value)
      if(accum.value == 0){
        println("任务完成")
        println(rdd.collect())
      }
    }
  }
}
