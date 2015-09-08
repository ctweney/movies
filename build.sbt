name := "movies"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.2" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"

initialCommands in console := """val sc = new org.apache.spark.SparkContext("local[4]", "movies")"""
