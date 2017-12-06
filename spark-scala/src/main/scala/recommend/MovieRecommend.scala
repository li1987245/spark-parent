package recommend

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * http://spark.apache.org/docs/latest/configuration.html
  * Created by jinwei on 17-11-1.
  */
object MovieRecommend {


  def areaUnderCurve(
                      positiveData: RDD[Rating],
                      bAllItemIDs: Broadcast[Array[Int]],
                      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive", and map to tuples
    val positiveUserProducts = positiveData.map(r => (r.user, r.product))
    // Make predictions for each of them, including a numeric score, and gather by user
    val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other items, excluding those that are "positive" for the user.
    val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
      // mapPartitions operates on many (user,positive-items) pairs at once
      userIDAndPosItemIDs => {
        // Init an RNG and the item IDs set once for partition
        val random = new Random()
        val allItemIDs = bAllItemIDs.value
        userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
          val posItemIDSet = posItemIDs.toSet
          val negative = new ArrayBuffer[Int]()
          var i = 0
          // Keep about as many negative examples per user as positive.
          // Duplicates are OK
          while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
            val itemID = allItemIDs(random.nextInt(allItemIDs.size))
            if (!posItemIDSet.contains(itemID)) {
              negative += itemID
            }
            i += 1
          }
          // Result is a collection of (user,negative-item) tuples
          negative.map(itemID => (userID, itemID))
        }
      }
    }.flatMap(t => t)
    // flatMap breaks the collections above down into one big set of tuples

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

    // Join positive and negative by user
    positivePredictions.join(negativePredictions).values.map {
      case (positiveRatings, negativeRatings) =>
        // AUC may be viewed as the probability that a random positive item scores
        // higher than a random negative one. Here the proportion of all positive-negative
        // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
        var correct = 0L
        var total = 0L
        // For each pairing,
        for (positive <- positiveRatings;
             negative <- negativeRatings) {
          // Count the correctly-ranked pairs
          if (positive.rating > negative.rating) {
            correct += 1
          }
          total += 1
        }
        // Return AUC: fraction of pairs ranked correctly
        correct.toDouble / total
    }.mean() // Return mean AUC over users
  }

  def predictMostListened(sc: SparkContext, train: RDD[Rating])(allData: RDD[(Int,Int)]) = {
    val bListenCount =
      sc.broadcast(train.map(r => (r.product, r.rating)).reduceByKey(_ + _).collectAsMap())
    allData.map { case (user, product) =>
      Rating(user, product, bListenCount.value.getOrElse(product, 0.0))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("movie recommend")
    val sc = new SparkContext(conf)
    //1.6
    val spark = new SQLContext(sc)
    import spark.implicits._
    //movieId,title,genres
    val movies = sc.textFile("hdfs://localhost:9000//data/recommend/movies.csv")
    val moviesRDD = movies.mapPartitionsWithIndex((index,iter)=>{
      if(index == 0)
        iter.drop(1)
      else
        iter
    })
    case class Movie(movieId: Int, title: String, genres: String) extends Serializable
    val moviesDF = moviesRDD.flatMap(line => {
      val pattern = "(\\d+),(.*?),(.*?)".r
      line match {
        case pattern(a, b, c) => Some(Movie(a.toInt, b.toString, c.toString))
        case _ => None
      }
    }).toDF()
    //movieId,imdbId,tmdbId
    val links = sc.textFile("hdfs://localhost:9000//data/recommend/links.csv")
    val linksRDD = links.mapPartitionsWithIndex((index,iter)=>{
      if(index == 0)
        iter.drop(1)
      else
        iter
    })
    case class Links(movieId: Int, imdbId: Int) extends Serializable
    val linksDF = linksRDD.flatMap(line => {
      val pattern = "(\\d+),(\\d+),(.*?)".r
      line match {
        case pattern(a, b, c) => Some(Links(a.toInt, b.toInt))
        case _ => None
      }
    }).toDF()
    moviesRDD.cache()
    moviesRDD.first()
    movies
    //userId,movieId,rating,timestamp
    val rating = sc.textFile("/data/recommend/ratings.csv")
    val ratingFirst = rating.first()
    val ratingRDD = rating.filter(!_.equals(ratingFirst))
    case class Rate(userId: Int, movieId: Int, rating: Double, timestamp: Int) extends Serializable
    val ratingDF = ratingRDD.flatMap(line => {
      val pattern = "(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?)".r
      line match {
        case pattern(a, a1, b, b1, c, c1, d, d1) => Some(Rate(a.toInt, b.toInt, c.toDouble, d.toInt))
        case _ => None
      }
    }).toDF()
    //推荐构成用户-产品-值的抽象
    val allData = ratingRDD.flatMap(line => {
      val pattern = "(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?),(\\d+(.\\d+)?)".r
      line match {
        case pattern(a, a1, b, b1, c, c1, d, d1) => Some(Rating(a.toInt, b.toInt, c.toDouble))
        case _ => None
      }
    })
    val Array(trainData,cvData) = allData.randomSplit(Array(0.9,0.1))
    val model = ALS.trainImplicit(trainData, 10, 5)
    //获取产品并去重
    val allItemIDs = allData.map(_.product).distinct().collect()
    val bAllItemIDs = sc.broadcast(allItemIDs)
    val auc0 = areaUnderCurve(cvData, bAllItemIDs, model.predict)
    val auc = areaUnderCurve(
      cvData, bAllItemIDs, predictMostListened(sc, trainData))
    //查看特征向量
    model.userFeatures.mapValues(_.mkString(", ")).first
    //Array[org.apache.spark.mllib.recommendation.Rating] = Array(Rating(370,5952,0.4246855038521251), Rating(370,7153,0.42325135980173734), Rating(370,3578,0.4207664342922038), Rating(370,2959,0.41735031553656765), Rating(370,4993,0.41167365870202915))
    model.recommendProducts(370, 5)
    ratingDF.where(ratingDF("userId") === 370).show()
    ratingDF.registerTempTable("rating")
    /**
      * userId|movieId|rating| timestamp|
      * +------+-------+------+----------+
      * |   370|   2762|   5.0|1096511467|
      * |   370|   2770|   4.0|1096496929|
      * |   370|   2840|   4.5|1096510977|
      * |   370|   2959|   4.5|1096511972|
      * |   370|   3033|   4.0|1096496974|
      * |   370|   3052|   4.5|1096512163|
      * |   370|   3147|   5.0|1096512119|
      * |   370|   3578|   4.5|1096511945|
      * |   370|   3994|   4.0|1096512193|
      * |   370|   3996|   4.0|1096512001|
      * |   370|   4022|   4.5|1096495888|
      * |   370|   4105|   5.0|1096510858|
      * |   370|   4226|   4.0|1096512091|
      * |   370|   4349|   4.0|1096510796|
      * |   370|   4735|   4.0|1096510873|
      * |   370|   4975|   3.5|1096511489|
      * |   370|   4993|   5.0|1096510955|
      * |   370|   5349|   4.5|1096512187|
      * |   370|   5378|   3.0|1096511438|
      * |   370|   5952|   5.0|1096510959
      */
    spark.sql("select * from rating where userId=370").show()
    moviesDF.registerTempTable("movies")
    //Drama|Thriller
    spark.sql("select * from movies where movieId=2840").show()
    //Action|Adventure|Drama|Fantasy
    spark.sql("select * from movies where movieId=7153").show()
    linksDF.registerTempTable("links")
    spark.sql("select * from movies join links on movies.movieId=links.movieId where movieId=5952").show()

  }
}
