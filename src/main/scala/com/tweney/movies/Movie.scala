package com.tweney.movies

import com.tweney.movies.util.Loggable

@SerialVersionUID(uid = 100L)
case class Movie(id: Int, name: String, genres: Set[String]) extends Loggable with Serializable {
}
