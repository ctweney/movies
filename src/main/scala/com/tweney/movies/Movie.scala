package com.tweney.movies

import com.tweney.movies.util.Loggable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

@SerialVersionUID(uid = 100L)
case class Movie(id: Int, name: String, genres: Set[String]) extends Loggable with Serializable {
}

object Movie extends Loggable {
  def rawData(sc: SparkContext, filename: String): RDD[String] = {
    logger.info("Loading movies from '" + filename + "'")
    sc.textFile(filename)
  }

  // movies.dat format:
  // MovieID::Title::Genres
  def moviesById(input: RDD[String]): RDD[(Int, Movie)] = {
    input
      .map(line => line.split("::"))
      .map(fields => (fields(0).toInt, new Movie(fields(0).toInt, fields(1), fields(2).split('|').toSet)))
  }

}
