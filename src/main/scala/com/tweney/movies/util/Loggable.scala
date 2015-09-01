package com.tweney.movies.util

import org.apache.log4j.Logger

trait Loggable {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
}
