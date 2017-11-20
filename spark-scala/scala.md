scala.collection.Iterable <=> Java.lang.Iterable
scala.collection.Iterable <=> java.util.Collection
scala.collection.Iterator <=> java.util.{ Iterator, Enumeration }
scala.collection.mutable.Buffer <=> java.util.List
scala.collection.mutable.Set <=> java.util.Set
scala.collection.mutable.Map <=> java.util.{ Map, Dictionary }
scala.collection.mutable.ConcurrentMap <=> java.util.concurrent.ConcurrentMap

scala.collection.Seq         => java.util.List
scala.collection.mutable.Seq => java.util.List
scala.collection.Set         => java.util.Set
scala.collection.Map         => java.util.Map
java.util.Properties         => scala.collection.mutable.Map[String, String]

当然这些转换均需要在scala文件中引入，scala.collection.JavaConversions._ 
scala中tuple、list为不可变对象（区别于java），每次操作会返回新的list
scala中set默认为不可变对象，如果需要可变set，需要导入import scala.collection.mutable.Set
scala 无三目运算 a>0?a:b 使用 if(a>0) a else b 代替


### spark
spark-shell --jars **.jar,**.jar --master yarn
./spark-submit --class ** --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=1024M" --driver-java-options -XX:MaxPermSize=1024m --driver-memory 4g --master yarn-client --executor-memory 2G --executor-cores 35 --num-executors 15  **.jar
```$xslt
val out = left union right //返回所有元素新的RDD //{1,2,3,3,3,4,5,6}  
val insterstions = left intersection right //返回RDD的交集 {3}  
val cartesian = left cartesian right //返回两个RDD的笛卡尔积 3*4 
```
- rdd to df
```
1.6
val config = new SparkConf()
val sc = new SparkContext(sc)
val rdd = sc.parallelize(data)
rdd.filter(x => {
  val arr = x.split(",")
  val idCard = arr(1).toString
  if (idCard.matches("\\d+"))
    true
  else
    false
}).map(x => {
  val arr = x.split(",")
  val name = arr(0).toString
  val idCard = arr(1).toString
  arr(0) + "," + arr(1)
}).coalesce(1).saveAsTextFile("")
val sqlContext = new SQLContext(sc)
//1.java反射创建df
case class Book(name: String, type0: Int, price: Int)
def parse_Book(line: String) : Option[Book] = {
  val apache_pattern = """^(\S+) (\d{3}) (\d{3})""".r

  line match {
    case apache_pattern(a, b, c) => {
      Some(Book(a, b.toInt, c.toInt))
    }
    case _ => None
  }
}
import sqlContext.implicits._
val book = rdd.map(_.split(",")).map(p => Book(p(0), p(1).trim.toInt, p(2).trim.toInt)).toDF()
//as String,df to dataset
book.as[String].flatMap(parse_Book)
//df to rdd
book.rdd
//2.指定schema
val schemaString = "name,type0,price"
val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0), p(1), p(2)))
val schemaRDD = sqlContext.createDataFrame(rowRDD, schema)
schemaRDD.filter(schemaRDD("price") contains "\\d+")
  .write.save("/tmp/user_idcard")
```
### hadoop
 ```$xslt
spark jobs and YARN will create logs on hdfs at /app-logs/<running User>/logs/application_1463538185607_99971

To know more details about logs we can run yarn logs -applicationId application_1463538185607_99971
```