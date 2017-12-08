### 推荐
1. 数据集
http://blog.csdn.net/qq_32453673/article/details/72593675
2. 数据预览
```
val selectedColumnName = df.columns(q) //pull the (q + 1)th column from the columns array
df.agg(min(selectedColumnName), max(selectedColumnName))
```
3. 归一化
4. 特征选取
5. 模型训练
6. 交叉测试
7. 模型应用


OneHotEncoder=String->IndexDouble->SparseVector
- StringIndexer
```
1)按String字符串出现次数降序排序；次数相同，自然顺序。
2)按降序顺序转换成DoubleIndex，默认从0.0开始。
// 设置输入列、输出列
val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex")
//String => IndexDouble
// fit函数：按出现次数降序排序；次数相同，自然顺序。
// 如 a 出现3次;c 出现2次;b 出现1次;d 出现1次
// transform函数：按降序顺序StringToDoubleIndex
// 如 a=>0.0 c=>1.0 b=>2.0 d=>3.0
val indexed = indexer.fit(df).transform(df)
```
- OneHotEncoder
```
// IndexDouble => SparseVector
// OneHotEncode:实际上是转换成了稀疏向量
// Spark源码: The last category is not included by default 最后一个种类默认不包含
// 和python scikit-learn's OneHotEncoder不同，scikit-learn's OneHotEncoder包含所有
val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
  // 设置最后一个是否包含
    .setDropLast(false)
//transform 转换成稀疏向量
val encoded = encoder.transform(indexed)
```
- VectorIndexer
```

```
- 嵌套查询
```
https://stackoverflow.com/questions/43817493/reading-nested-json-via-spark-sql-analysisexception-cannot-resolve-column
{  
   "parent":[  
      {  
         "prop1":1.0,
         "prop2":"C",
         "children":[  
            {  
               "child_prop1":[  
                  "3026"
               ]
            }
         ]
      }
   ]
}
import org.apache.spark.sql.functions.explode

val data = spark.read.json("src/test/java/data.json")
val child = data.select(explode(data("parent.children"))).toDF("children")

child.select(explode(child("children.child_prop1"))).toDF("child_prop1").show()
```