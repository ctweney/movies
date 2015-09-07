package com.tweney.movies

import com.tweney.movies.util.Loggable

@SerialVersionUID(uid = 100L)
case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Int) extends Loggable with Serializable {
}
