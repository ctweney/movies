package com.tweney.movies.spark_tests

import com.tweney.movies.MovieReader

class MovieReaderTest extends SparkTest {

  def reader = {
    new MovieReader(sc, "data/movielens_tiny")
  }

  "a reader in the default data directory" should "still be able to read data" in {
    val defaultReader = new MovieReader(sc)
    defaultReader.moviesById().collect().length should be(3883)
  }

  "a raw movies.dat file" should "be read into an RDD of String" in {
    val movies = reader.rawMovies()
    movies.take(1)(0) should be("1::Toy Story (1995)::Animation|Children's|Comedy")
  }

  "a set of raw data lines" should "convert to a keyed RDD of Movie instances" in {
    val movies = reader.moviesById()
    movies.first()._1 should be(1)
    movies.first()._2.name should be("Toy Story (1995)")
    movies.collect().length should be(50)
  }

  "a raw ratings.dat file" should "be read into an RDD of String" in {
    val ratings = reader.rawRatings()
    ratings.take(1)(0) should be("1::1193::5::978300760")
  }

  "a set of raw data lines" should "convert to a keyed RDD of Rating instances" in {
    val data = reader.rawRatings()
    val ratings = reader.ratingsByMovieId()
    ratings.first()._1 should be(1193)
    ratings.first()._2.userId should be(1)
    ratings.first()._2.movieId should be(1193)
    ratings.first()._2.rating should be(5)
    ratings.collect().length should be(500)
  }

  "averaging the ratings using the functional method" should "return averages" in {
    val averages = reader.averages()
    averages.first()._1 should be("Babe (1995)")
    averages.first()._2 should be(4.0)
  }

  "the functional average and sql average methods" should "agree with one another" in {
    val functionalAverages = reader.averages()
    val sqlAverages = reader.averagesBySQL()
    functionalAverages.first()._1 should be("Babe (1995)")
    sqlAverages.filter("name = 'Babe (1995)'").take(1)(0)(1) should be(functionalAverages.first()._2)
  }

}
