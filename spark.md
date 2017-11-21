### Spark on YARN的部署
wget https://d3kbcqa49mib13.cloudfront.net/spark-1.6.3-bin-hadoop2.6.tgz
tar -xvzf spark-1.6.3-bin-hadoop2.6.tgz
mv spark-1.6.3-bin-hadoop2.6 spark-1.6.2
cp conf/spark-env.sh.template conf/spark-env.sh
cp conf/slaves.template conf/slaves
vim conf/spark-env.sh
```markdown
# JDK目录
export JAVA_HOME=/opt/java/jdk1.8.0_121
# Scala目录
export SCALA_HOME=/usr/local/scala/scala-2.11.8
# Master IP地址
export SPARK_MASTER_IP=localhost
# Worker运行内存
export SPARK_WORKER_MEMORY=2G
# hadoop配置文件目录 cdh中默认是如下目录 这个hadoop必须运行在yarn上 spark才能直接通过此配置文件目录通过yarn进行调度
export HADOOP_CONF_DIR=/opt/hadoop-2.7.3/etc/hadoop
# spark master端口 默认7077 下面是可选的
export SPARK_MASTER_PORT=7077
# 此项默认 也是可选的
export MASTER=spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT}
# spark 1.6开启ipython
export IPYTHON=1
```

hive with spark
- 编译spark without hive
```
wget https://d3kbcqa49mib13.cloudfront.net/spark-1.6.3.tgz
./make-distribution.sh --name "hadoop2-without-hive" --tgz "-Pyarn,hadoop-provided,hadoop-2.6,parquet-provided,scala-2.11"
tar xzf spark-1.6.3-bin-hadoop2-without-hive.tgz -C /opt
```
- Failed to create spark client
hive --hiveconf hive.root.logger=DEBUG,console
```markdown
set hive.execution.engine=spark;
set spark.master=spark://localhost:7077;
set spark.eventLog.enabled=true;
set spark.eventLog.dir=/tmp/hadoop;
set spark.executor.memory=512m;
set spark.serializer=org.apache.spark.serializer.KryoSerializer;
```
- [Shuffle](https://www.iteblog.com/archives/1964.html)
```
Stage 之间的数据交互是需要 Shuffle 来完成
shuffle阶段消耗CPU、RAM、磁盘还有网络资源
Spark（2.2.0） 内置只支持一种 Shuffle 实现：org.apache.spark.shuffle.sort.SortShuffleManager，通过参数 spark.shuffle.manager 来配置。这是标准的 Spark Shuffle 实现，其内部实现依赖了 Netty 框架

```
![](https://www.iteblog.com/pic/spark/spark-shuffle_iteblog.png)

### 使用
#### spark RDD&DataFrame
- spark submit
```markdown
bin/spark-submit --class path.to.your.Class --master yarn --deploy-mode cluster [options] <app jar> [app options]
spark-submit --master yarn --deploy-mode client streaming.py #python 
```
- DataFrame：
```
text:
textfile=spark.read.text('hdfs://localhost:9000/tmp/tpcds-generate/2/web_page/data-m-00001')
csv:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('sql').getOrCreate()
df = spark.read.csv('hdfs://localhost:9000/tmp/tpcds-generate/2/web_page/data-m-00001',sep='|')
```
- RDD
```
rdd = sc.parallelize(json/list)
rdd=sc.textFile('hdfs://localhost:9000/tmp/tpcds-generate/2/web_page/data-m-00001')
```
- dataset
```
import spark.implicits._ 
case class Person(name: String, age: Long) 
val data = Seq(Person("Bob", 21), Person("Mandy", 22), Person("Julia", 19)) val ds = spark.createDataset(data)
```
- dataset to rdd
```
val dsToRDD = ds.rdd
```
- rdd to dataset
```
val rdd = sc.textFile("data.txt") 
val ds = spark.createDataset(rdd)
```
- df to dataset
```
case class T(name: String, age: Long) 
val rDDToDataSet = rddToDF.as[T]
```
- DataFrame转RDD
```markdown
df=textfile.rdd
```
- RDD to DataFrame
```
>>> l = [('Alice', 1)]
>>> sqlContext.createDataFrame(l).collect()
[Row(_1=u'Alice', _2=1)]
>>> sqlContext.createDataFrame(l, ['name', 'age']).collect()
[Row(name=u'Alice', age=1)]

>>> d = [{'name': 'Alice', 'age': 1}]
>>> sqlContext.createDataFrame(d).collect()
[Row(age=1, name=u'Alice')]

>>> rdd = sc.parallelize(l)
>>> sqlContext.createDataFrame(rdd).collect()
[Row(_1=u'Alice', _2=1)]
>>> df = sqlContext.createDataFrame(rdd, ['name', 'age'])
>>> df.collect()
[Row(name=u'Alice', age=1)]

>>> from pyspark.sql import Row
>>> Person = Row('name', 'age')
>>> person = rdd.map(lambda r: Person(*r))
>>> df2 = sqlContext.createDataFrame(person)
>>> df2.collect()
[Row(name=u'Alice', age=1)]

scala:
val rdd = sc.parallelize(List(("tom",18),("lilly",20),("sam",10)))
case class Person(name:String,age:Int)
import spark.implicits._
val df=rdd.map(x=>Person(x._1,x._2)).toDF

def dfSchema(columnNames: List[String]): StructType = StructType( Seq( StructField(name = "name", dataType = StringType, nullable = false), StructField(name = "age", dataType = IntegerType, nullable = false) ) ) 
def row(line: List[String]): Row = Row(line(0), line(1).toInt) 
val schema = dfSchema(Seq("name", "age")) 
val data = rdd.map(_.split(",").to[List]).map(row) 
val dataFrame = spark.createDataFrame(data, schema)

```
- workcount
```markdown
textfile=sc.textFile('hdfs://localhost:9000/tmp/tpcds-generate/2/web_page/data-m-00001')
counts = textfile.flatMap(lambda x: x.split('|')).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
scala
rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).filter(_._2 > 5).sortBy(_._2,false).collect.foreach(println)
```
- mapPartitionsWithIndex
```
val data = rdd 
.mapPartitionsWithIndex((index, element) => if (index == 0) it.drop(1) else it) // skip header 
.map(_.split(",").to[List]) .map(row)
```
- sortBy
```
第一个参数是一个函数，该函数的也有一个带T泛型的参数，返回类型和RDD中元素的类型是一致的；
第二个参数是ascending，从字面的意思大家应该可以猜到，是的，这参数决定排序后RDD中的元素是升序还是降序，默认是true，也就是升序；
第三个参数是numPartitions，该参数决定排序后的RDD的分区个数，默认排序后的分区个数和排序之前的个数相等，即为this.partitions.size。
rdd.sortBy(_._2,false)或rdd.sortBy(x=>x._2,false)
```
- orderByKey
```
sortByKey函数作用于Key-Value形式的RDD，并对Key进行排序。两个参数，同sortBy第二、三个参数
rdd.sortByKey(false,2).collect
通过实现隐式函数从写排序规则
implicit val sortIntegerByString = new Ordering[String]{
| override def compare(a:String,b:String)=
| a.compare(b)} 
rdd.sortByKey().collect
```
#### spark sql
- 过滤
```markdown
df.filter(df._c2=='1999-09-04').count()
```
- join
```markdown
df.join(df1,df._c0==df1._c11).count()
df.join(df1,df._c0==df1._c11,'outer').count()
1. Broadcast Join
Broadcast Join的条件有以下几个：
被广播的表需要小于 spark.sql.autoBroadcastJoinThreshold 所配置的值，默认是10M （或者加了broadcast join的hint）
基表不能被广播，比如 left outer join 时，只能广播右表
禁用广播sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

```
- 统计
```markdown
df.groupby('_c2').agg({"_c2":"count"}).collect()
df.groupby('_c2').agg(F.countDistinct('_c2')).collect()#分组去重统计
df.groupBy('_c2').count().collect()#分组统计简写
```
- sql
```markdown
df.createOrReplaceTempView('web_site')
sqlDF=spark.sql('select * from web_site limit 1')
sqlDF.show()
```
- spark sql
```markdown
spark-sql --master yarn  --driver-cores 1 --hiveconf "spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse" 
$SPARK_HOME/sbin/start-thriftserver.sh --master yarn    --driver-java-options "-Dspark.driver.port=4050" --hiveconf "hive.server2.thrift.port=10000"  --hiveconf "hive.metastore.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse"
$SPARK_HOME/bin/beeline --hiveconf hive.server2.thrift.port=10000 --hiveconf "hive.metastore.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse"
beeline> !connect jdbc:hive2://localhost:10000
0: jdbc:hive2://localhost:10000> !quit
增加hive配置后可直接访问hive数据(元数据)
the hive.metastore.warehouse.dir property in hive-site.xml is deprecated since Spark 2.0.0. Instead, use spark.sql.warehouse.dir to specify the default location of database in warehouse.
```
-hive
```markdown
将Hive中的hive-site.xml文件拷贝到Spark的conf目录下
pyspark --jars /home/jinwei/tool/mysql-connector-java-5.1.43.jar
sqlContext.sql("show databases").show()
```
- jdbc连接其他数据源
```markdown
bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
jdbcDF = spark.read.format("jdbc").options(
  Map("url" ->  "jdbc:mysql://localhost:3306/zh_mydemo?user=root&password=admin",
  "dbtable" -> "zh_mydemo.company",
  "fetchSize" -> "10000",
  "partitionColumn" -> "yeard", "lowerBound" -> "1988", "upperBound" -> "2016", "numPartitions" -> "28"
  )).load()
jdbcDF.createOrReplaceTempView("company")

pyspark --driver-class-path mysql-connector-java-5.1.43.jar --jars /home/jinwei/tool/mysql-connector-java-5.1.43.jar
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306") \
    .option("dbtable", "zh_mydemo.company") \
    .option("user", "root") \
    .option("password", "admin") \
    .load() #jdbcDF为dataframe
jdbcDF.groupby("companyLevel").count().show() #查询dbtable数据
jdbcDF2 = spark.read \
    .jdbc("jdbc:mysql://localhost:3306", "schema.tablename",
          properties={"user": "username", "password": "password","fetchSize":"10000"})
jdbcDF.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "zh_mydemo.company") \
    .option("user", "username") \
    .option("password", "password") \
    .save()
l=[{"id":11,"name":u"测试","parentId":2,"companyLevel":3}] #中文字符串要明确unicode编码，否则乱码
df=sqlContext.createDataFrame(l)
df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306?useUnicode=true&characterEncoding=UTF-8") \
    .option("dbtable", "zh_mydemo.company") \
    .option("user", "root") \
    .option("password", "admin") \
    .save(mode="append")

jdbcDF.write \
        .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)") \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})
```
- 自定义schema
```markdown
from pyspark.sql import Row
lines = sc.textFile("hdfs://localhost:9000/tmp/tpcds-generate/2/web_page/data-m-00001")
parts = lines.map(lambda l: l.split("|"))
web_site = parts.map(lambda p: Row(key=p[0], value=''.join(p[1:])))
schemaWebSite = spark.createDataFrame(web_site) #dataframe
schemaWebSite.createOrReplaceTempView("web_site")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT key,value FROM web_site WHERE key <= 13")

# The results of SQL queries are Dataframe objects.
# rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
teenNames = teenagers.rdd.map(lambda p: "value: " + p.value).collect()
for name in teenNames:
    print(name)
```
- 加载/保存
```markdown
df=spark.read.load("file:///opt/spark-2.2.0/examples/src/main/resources/users.parquet") #默认是parquet类型，其他类型需要指定格式format="json"
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet", format="json")
df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`") #简化版
df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
```
#### spark streaming
- streaming
```
from __future__ import print_function
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
sparkConf = SparkConf()
sc = SparkContext(appName="stream",conf=sparkConf)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc,1)
lines = ssc.socketTextStream("localhost", 9999) #Dstream
words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
words.pprint()
ssc.start()
ssc.awaitTermination()
```
- Structured Streaming
```markdown


```


### 问题
```markdown
1.RDD或DataFrame使用foreach打印，在2.7环境
In [18]: textfile.foreach(print)
SyntaxError: invalid syntax
解决方案：from __future__ import print_function
2. Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME
To make Spark runtime jars accessible from YARN side, you can specify spark.yarn.archive or spark.yarn.jars. 
For details please refer to Spark Properties. If neither spark.yarn.archive nor spark.yarn.jars is specified, 
Spark will create a zip file with all jars under $SPARK_HOME/jars and upload it to the distributed cache.
hadoop fs -mkdir -p hdfs:///tmp/spark/lib_jars/
hadoop fs -put  $SPARK_HOME/jars/* hdfs:///tmp/spark/lib_jars/
vim $SPARK_HOME/conf/spark-defaults.conf
添加spark.yarn.jars hdfs:///tmp/spark/lib_jars/*.jar

```


- Performance Tuning
```markdown
Caching Data In Memory
Spark SQL can cache tables using an in-memory columnar format by calling spark.catalog.cacheTable("tableName") or dataFrame.cache(). Then Spark SQL will scan only required columns and will automatically tune compression to minimize memory usage and GC pressure. You can call spark.catalog.uncacheTable("tableName") to remove the table from memory.

Configuration of in-memory caching can be done using the setConf method on SparkSession or by running SET key=value commands using SQL.

Property Name	Default	Meaning
spark.sql.inMemoryColumnarStorage.compressed	true	When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.sql.inMemoryColumnarStorage.batchSize	10000	Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data.
Other Configuration Options
The following options can also be used to tune the performance of query execution. It is possible that these options will be deprecated in future release as more optimizations are performed automatically.

Property Name	Default	Meaning
spark.sql.files.maxPartitionBytes	134217728 (128 MB)	The maximum number of bytes to pack into a single partition when reading files.
spark.sql.files.openCostInBytes	4194304 (4 MB)	The estimated cost to open a file, measured by the number of bytes could be scanned in the same time. This is used when putting multiple files into a partition. It is better to over estimated, then the partitions with small files will be faster than partitions with bigger files (which is scheduled first).
spark.sql.broadcastTimeout	300	
Timeout in seconds for the broadcast wait time in broadcast joins

spark.sql.autoBroadcastJoinThreshold	10485760 (10 MB)	Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. Note that currently statistics are only supported for Hive Metastore tables where the command ANALYZE TABLE <tableName> COMPUTE STATISTICS noscan has been run.
spark.sql.shuffle.partitions	200	Configures the number of partitions to use when shuffling data for joins or aggregations.
```