package com.tweney.movies.spark_tests

import com.tweney.movies.{MovieReader, Movie}

class MovieReaderTest extends SparkTest {

  def reader = {
    new MovieReader(sc)
  }

  "a raw movies.dat file" should "be read into an RDD of String" in {
    val movies = reader.rawMovies()
    movies.take(1)(0) should be("1::Toy Story (1995)::Animation|Children's|Comedy")
  }

  "a set of raw data lines" should "convert to a keyed RDD of Movie instances" in {
    val movies = reader.moviesById()
    movies.first()._1 should be(1)
    movies.first()._2.name should be("Toy Story (1995)")
    movies.first()._2.genres should be(Set[String]("Animation", "Children's", "Comedy"))
    movies.collect().length should be(3883)
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
    ratings.collect().length should be(1000209)
  }

  "averaging the ratings of movies with raw functional paradigm" should "return averages" in {
    val movies = reader.rawMovies()
      .map(line => line.split("::"))
      .map(field => (field(0).toInt, field(1)))
    val ratings = reader.rawRatings()
      .map(line => line.split("::"))
      .map(field => (field(1).toInt, field(2).toInt))

    val averages = movies.join(ratings)
      .aggregateByKey((0, 0, ""))(
        (acc, value) => (acc._1 + value._2, acc._2 + 1, value._1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3))
      .mapValues(sumCountName => (sumCountName._3, 1.0 * sumCountName._1 / sumCountName._2))
    averages.take(20).foreach(l => println(l))
    averages.first()._2._1 should be("Bonnie and Clyde (1967)")
    averages.first()._2._2 should be(4.096209912536443)
  }

  "averaging the ratings using the class method" should "return averages" in {
    val averages = reader.averages()
    averages.take(20).foreach(l => println(l))
    averages.first()._1 should be("Bonnie and Clyde (1967)")
    averages.first()._2 should be(4.096209912536443)
  }
}
