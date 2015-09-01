package com.tweney.movies

import com.tweney.movies.util.Loggable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

@SerialVersionUID(uid = 100L)
case class Movie(id: Int, name: String, genres: Set[String]) extends Loggable with Serializable {
}

object Movie extends Loggable {
  def loadData(sc: SparkContext, filename: String): RDD[String] = {
    logger.info("Loading movies from '" + filename + "'")
    sc.textFile(filename)
  }

  // movies.dat format:
  // MovieID::Title::Genres
  def convertData(input: RDD[String]): RDD[Movie] = {
    input
      .map(line => line.split("::"))
      .map(fields => new Movie(
      fields(0).toInt,
      fields(1),
      fields(2).split('|').toSet))
  }

  def moviesById(input: RDD[Movie]): RDD[(Int, Movie)] = {
    input
      .map(movie => (movie.id, movie))
  }
}
