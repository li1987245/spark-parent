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