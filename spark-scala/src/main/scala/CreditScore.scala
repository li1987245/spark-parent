
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import scala.collection.mutable.Set

/**
  * 信用评分
  *
  */
object CreditScore {

  case class User(level: String, gender: String, age: Int, online_time: Int, integral: Double, pay: Double, pay_time: Int, phone_cost: Double, phone_time: Int)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .appName("credit score")
      .getOrCreate()
    import spark.implicits._

    // Load training data
    val training = spark.read.csv("/tmp/train.csv")
    training.head(5).map(x => {
      val v = x.getInt(5)
      println(v.getClass.getSimpleName)
      v
    }
    )
    training.map(_.getInt(5)).head()
    val data = training.na.drop(Array("_c2", "_c3", "_c5", "_c7", "_c9", "_c12", "_c13", "_c17", "_c19")).map(row => {
      User(row.getString(2), row.getString(3), row.getString(5).toInt, row.getString(7).toInt, row.getString(9).toDouble, row.getString(12).toDouble, row.getString(13).toInt, row.getString(17).toDouble, row.getString(19).toInt)
    })
    val Array(train_data,test_data) = data.randomSplit(Array(0.9, 0.1))
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.3,0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test_data)
      .select("features", "label", "prediction")
      .show()

    // Print the coefficients and intercept for multinomial logistic regression
//    println(s"Coefficients: \n${lrModel.coefficientMatrix}")
//    println(s"Intercepts: \n${lrModel.interceptVector}")
  }
}
