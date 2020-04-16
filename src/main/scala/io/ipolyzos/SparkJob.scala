package io.ipolyzos

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

abstract class SparkJob(appName: String) extends App {
  val logger = Logger.getLogger(getClass.getName)

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.auth.username" , "cassandra")
    .config("spark.cassandra.auth.password" , "cassandra")
    .getOrCreate()
}
