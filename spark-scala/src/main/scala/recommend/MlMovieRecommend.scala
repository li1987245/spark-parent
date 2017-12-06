package recommend

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * http://spark.apache.org/docs/latest/configuration.html
  * Created by jinwei on 17-11-1.
  */
object MlMovieRecommend {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("movie recommend")
    val sc = new SparkContext(conf)
    //1.6
    val spark = new SQLContext(sc)
    import spark.implicits._
    //userId,movieId,rating,timestamp
    val rating = sc.textFile("/data/recommend/ratings.csv")
    val ratingFirst = rating.first()
    case class Rate(userId: Int, movieId: Int, rating: Float) extends Serializable
    val ratingRDD = rating.filter(!_.equals(ratingFirst)).flatMap(line => {
      val pattern = "(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?)".r
      line match {
        case pattern(a, a1, b, b1, c, c1, d, d1) => Some(Rate(a.toInt, b.toInt, c.toFloat))
        case _ => None
      }
    })
    //    val ratingRDD = rating.filter(!_.equals(ratingFirst)).flatMap(line => {
    //      val pattern = "(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?)".r
    //      line match {
    //        case pattern(a, a1, b, b1, c, c1, d, d1) => Some(Rating(a.toInt, b.toInt, c.toFloat))
    //        case _ => None
    //      }
    //    })
    val ratingDF = ratingRDD.toDF()
    val allData = spark.createDataset(ratingRDD)
    val Array(trainRDD, cvRDD) = ratingRDD.randomSplit(Array(0.9, 0.1))
    //推荐构成用户-产品-值的抽象
    val trainData = trainRDD.toDF()
    val cvData = cvRDD.toDF()
    /*
       * (1)秩Rank：模型中隐含因子的个数：低阶近似矩阵中隐含特在个数，因子一般多一点比较好，
       * 但是会增大内存的开销。因此常在训练效果和系统开销之间进行权衡，通常取值在10-200之间。
       * (2)最大迭代次数：运行时的迭代次数，ALS可以做到每次迭代都可以降低评级矩阵的重建误差，
       * 一般少数次迭代便能收敛到一个比较合理的好模型。
       * 大部分情况下没有必要进行太对多次迭代（10次左右一般就挺好了）
       * (3)正则化参数regParam：和其他机器学习算法一样，控制模型的过拟合情况。
       * 该值与数据大小，特征，系数程度有关。此参数正是交叉验证需要验证的参数之一。
       */
    val als = new ALS().setMaxIter(8).setRank(20).setRegParam(0.8).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setPredictionCol("prediction");
    // Configure an ML pipeline, which consists of one stage
    //一般会包含多个stages
    val pipeline = new Pipeline().setStages(Array(als))
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    val paramGrid: Array[ParamMap] = new ParamGridBuilder().addGrid(als.rank, Array(5, 10, 20)).addGrid(als.regParam, Array(0.05, 0.10, 0.15, 0.20, 0.40, 0.80)).build()

    // CrossValidator 需要一个Estimator,一组Estimator ParamMaps, 和一个Evaluator.
    // （1）Pipeline作为Estimator;
    // （2）定义一个RegressionEvaluator作为Evaluator，并将评估标准设置为“rmse”均方根误差
    // （3）设置ParamMap
    // （4）设置numFolds

    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator().setLabelCol("rating").setPredictionCol("prediction").setMetricName("rmse")).setEstimatorParamMaps(paramGrid).setNumFolds(5);

    // 运行交叉检验，自动选择最佳的参数组合
    val cvModel: CrossValidatorModel = cv.fit(trainData);
    //保存模型
    cvModel.save("/data/recommend/cvModel_als.modle");

    //System.out.println("numFolds: "+cvModel.getNumFolds());
    //Test数据集上结果评估
    //setLabelCol要和als设置的setRatingCol一致，不然会报错
    //setMetricName这个方法，评估方法的名字，一共有:
    //rmse-平均误差平方和开根号
    //mse-平均误差平方和
    //mae-平均距离（绝对
    val predictions = cvModel.transform(cvData);
    val evaluator: RegressionEvaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction");
    val rmse = evaluator.evaluate(predictions);
    println("RMSE @ test dataset " + rmse);
  }
}
