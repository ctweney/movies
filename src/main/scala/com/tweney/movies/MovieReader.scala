package com.tweney.movies

import com.tweney.movies.util.Loggable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MovieReader(sc: SparkContext) extends Loggable {

  val movieFilename = "data/movielens/movies.dat"
  val ratingFilename = "data/movielens/ratings.dat"

  def rawData(filename: String): RDD[String] = {
    logger.info("Loading data from '" + filename + "'")
    sc.textFile(filename)
      .cache()
  }

  def rawMovies(): RDD[String] = {
    rawData(movieFilename)
      .cache()
  }

  def rawRatings(): RDD[String] = {
    rawData(ratingFilename)
      .cache()
  }

  // movies.dat format:
  // MovieID::Title::Genres
  def moviesById(): RDD[(Int, Movie)] = {
    rawMovies()
      .map(line => line.split("::"))
      .map(fields => (fields(0).toInt, new Movie(fields(0).toInt, fields(1), fields(2).split('|').toSet)))
      .cache()
  }

  // ratings.dat format:
  // UserID::MovieID::Rating::Timestamp
  def ratingsByMovieId(): RDD[(Int, Rating)] = {
    rawRatings()
      .map(line => line.split("::"))
      .map(fields => (fields(1).toInt, new Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt)))
      .cache()
  }

  def moviesByIdWithRating(): RDD[(Int, (Movie, Rating))] = {
    moviesById()
      .join(ratingsByMovieId())
      .cache()
  }

  def averages(): RDD[(String, Double)] = {
    moviesByIdWithRating().aggregateByKey((0, 0, ""))(
      (acc, value) => (acc._1 + value._2.rating, acc._2 + 1, value._1.name),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3))
      .mapValues(sumCountName => (sumCountName._3, 1.0 * sumCountName._1 / sumCountName._2))
      .map(row => (row._2._1, row._2._2))
  }

}
