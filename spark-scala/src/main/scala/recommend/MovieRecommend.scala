package recommend

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * http://spark.apache.org/docs/latest/configuration.html
  * Created by jinwei on 17-11-1.
  */
object MovieRecommend {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("movie recommend")
    val sc = new SparkContext(conf)
    //2.0
    val spark = SparkSession.builder().appName("movie recommend").config("spark.shuffle.compress", "true").getOrCreate()
    import spark.implicits._
    case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
    val ratings = spark.read.option("header", true).csv("/data/recommend/ratings.csv").map(row => {
      Rating(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toFloat, row.getString(3).toLong)
    })
    //val movies = spark.read.csv("file:///home/jinwei/data/recommend/Recommend-movies-master/ml-latest-small/movies.csv")
    ratings.printSchema()
    //mkString把数组按照指定分隔符组成字符串
    //    println(ratings.first().mkString("\t"))
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    /*
      * (1)秩Rank：模型中隐含因子的个数：低阶近似矩阵中隐含特在个数，因子一般多一点比较好，
      * 但是会增大内存的开销。因此常在训练效果和系统开销之间进行权衡，通常取值在10-200之间。
      * (2)最大迭代次数：运行时的迭代次数，ALS可以做到每次迭代都可以降低评级矩阵的重建误差，
      * 一般少数次迭代便能收敛到一个比较合理的好模型。
      * 大部分情况下没有必要进行太对多次迭代（10次左右一般就挺好了）
      * (3)正则化参数regParam：和其他机器学习算法一样，控制模型的过拟合情况。
      * 该值与数据大小，特征，系数程度有关。此参数正是交叉验证需要验证的参数之一。
      */
    val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    //一般会包含多个stages
    val pipeline = new Pipeline().setStages(Array(als))
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    val paramGrid: Array[ParamMap] = new ParamGridBuilder().addGrid(als.rank, Array(5, 10, 20)).addGrid(als.regParam, Array(0.01, 0.05, 0.10)).build()

    // CrossValidator 需要一个Estimator,一组Estimator ParamMaps, 和一个Evaluator.
    // （1）Pipeline作为Estimator;
    // （2）定义一个RegressionEvaluator作为Evaluator，并将评估标准设置为“rmse”均方根误差
    // （3）设置ParamMap
    // （4）设置numFolds

    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator().setLabelCol("rating").setPredictionCol("prediction").setMetricName("rmse")).setEstimatorParamMaps(paramGrid).setNumFolds(5);

    // 运行交叉检验，自动选择最佳的参数组合
    val model: CrossValidatorModel = cv.fit(training)
    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)
    //需要删除predictions中NAN数据，否则rmse结果为NAN
    val prediction = predictions.na.drop()

    /**
      * Test数据集上结果评估
      * setLabelCol要和als设置的setRatingCol一致，不然会报错
      * setMetricName这个方法，评估方法的名字，一共有:
      * rmse-平均误差平方和开根号
      * mse-平均误差平方和
      * mae-平均距离
      */
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val rmse = evaluator.evaluate(prediction)
    //0.8134935553172877 0.8146742116731801
    println(s"Root-mean-square error = $rmse")

  }
}
