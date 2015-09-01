package com.tweney.movies.spark_tests

import com.tweney.movies.util.Loggable
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

abstract class SparkTest extends FlatSpec with BeforeAndAfter with Matchers with Loggable {

  private val master = "local[2]"
  private val appName = "test-spark"

  var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

}
