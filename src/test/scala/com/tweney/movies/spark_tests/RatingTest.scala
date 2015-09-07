package com.tweney.movies.spark_tests

import com.tweney.movies.Rating

class RatingTest extends SparkTest {

  val filename = "data/movielens/ratings.dat"

  "a Rating" should "be constructed" in {
    val rating = new Rating(1, 2, 3, 4)
    rating.rating should be(3)
  }

  "a raw ratings.dat file" should "be read into an RDD of String" in {
    val ratings = Rating.rawData(sc, filename)
    ratings.take(1)(0) should be("1::1193::5::978300760")
  }

  "a set of raw data lines" should "convert to a keyed RDD of Rating instances" in {
    val data = Rating.rawData(sc, filename)
    val ratings = Rating.ratingsByMovieId(data)
    ratings.first()._1 should be(1193)
    ratings.first()._2.userId should be(1)
    ratings.first()._2.movieId should be(1193)
    ratings.first()._2.rating should be(5)
    ratings.collect().length should be(1000209)
  }

}
