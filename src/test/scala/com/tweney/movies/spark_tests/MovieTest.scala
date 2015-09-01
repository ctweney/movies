package com.tweney.movies.spark_tests

import com.tweney.movies.Movie

class MovieTest extends SparkTest {

  val filename = "data/movielens/movies.dat"

  "a Movie" should "be constructed" in {
    val film = new Movie(1, "The Big Lebowski", Set[String]("Comedy", "Noir"))
    film.name should be("The Big Lebowski")
  }

  "a raw movies.dat file" should "be read into an RDD of String" in {
    val movies = Movie.loadData(sc, filename)
    movies.take(1)(0) should be("1::Toy Story (1995)::Animation|Children's|Comedy")
  }

  "a set of raw data lines" should "convert to an RDD of Movie instances" in {
    val data = Movie.loadData(sc, filename)
    val movies = Movie.convertData(data)
    movies.first().name should be("Toy Story (1995)")
    movies.first().genres should be(Set[String]("Animation", "Children's", "Comedy"))
    movies.collect().length should be(3883)
  }

  "a set of raw data" should "convert to a keyed RDD of Movies" in {
    val data = Movie.loadData(sc, filename)
    val movies = Movie.moviesById(Movie.convertData(data))
    val first = movies.take(1)(0)
    movies.collect().length should be(3883)
    first._1 should be(1)
    first._2.name should be("Toy Story (1995)")
  }
}
