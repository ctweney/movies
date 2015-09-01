package com.tweney.movies.spark_tests

import com.tweney.movies.Rating

class RatingTest extends SparkTest {

  val filename = "data/movielens/ratings.dat"

  "a Rating" should "be constructed" in {
    val rating = new Rating(1, 2, 3, 4)
    rating.rating should be(3)
  }

  "a raw ratings.dat file" should "be read into an RDD of String" in {
    val ratings = Rating.loadData(sc, filename)
    ratings.take(1)(0) should be("1::1193::5::978300760")
  }

  "a set of raw data lines" should "convert to an RDD of Rating instances" in {
    val data = Rating.loadData(sc, filename)
    val ratings = Rating.convertData(data)
    ratings.first().userId should be(1)
    ratings.first().movieId should be(1193)
    ratings.first().rating should be(5)
    ratings.collect().length should be(1000209)
  }

}
