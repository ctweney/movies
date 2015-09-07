package com.tweney.movies

import com.tweney.movies.util.Loggable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

@SerialVersionUID(uid = 100L)
case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Int) extends Loggable with Serializable {
}

object Rating extends Loggable {
  def rawData(sc: SparkContext, filename: String): RDD[String] = {
    logger.info("Loading ratings from '" + filename + "'")
    sc.textFile(filename)
  }

  // ratings.dat format:
  // UserID::MovieID::Rating::Timestamp
  def ratingsByMovieId(input: RDD[String]): RDD[(Int, Rating)] = {
    input
      .map(line => line.split("::"))
      .map(fields => (fields(1).toInt, new Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt)))
  }
}
