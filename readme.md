spark-submit  --class CalculateIdCard  --master yarn --deploy-mode cluster --conf spark.default.parallelism=1000 --driver-memory 4g   --executor-memory 4g --executor-cores 2  --num-executors 35   *.jar

spark-shell --master yarn --conf spark.default.parallelism=1200 --driver-memory 8g   --executor-memory 8g

- cache和persist的区别
```
cache和persist的区别：cache只有一个默认的缓存级别MEMORY_ONLY ，而persist可以根据情况设置其它的缓存级别。
useDisk：使用硬盘（外存）
useMemory：使用内存
useOffHeap：使用堆外内存，这是Java虚拟机里面的概念，堆外内存意味着把内存对象分配在Java虚拟机的堆以外的内存，这些内存直接受操作系统管理（而不是虚拟机）。这样做的结果就是能保持一个较小的堆，以减少垃圾收集对应用的影响。
deserialized：反序列化，其逆过程序列化（Serialization）是java提供的一种机制，将对象表示成一连串的字节；而反序列化就表示将字节恢复为对象的过程。序列化是对象永久化的一种机制，可以将对象及其属性保存起来，并能在反序列化后直接恢复这个对象
replication：备份数（在多个节点上备份）
```
- spark.default.parallelism
```markdown
设置spark.default.parallelism参数，防止spark计算出来的partition非常巨大
```
- return
```markdown
foreach中return会导致java.io.NotSerializableException: java.lang.Object
```
