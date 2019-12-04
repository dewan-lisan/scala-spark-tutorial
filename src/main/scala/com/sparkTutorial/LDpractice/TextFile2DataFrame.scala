package com.sparkTutorial.LDpractice

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TextFile2DataFrame extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val mySparkSession: SparkSession = SparkSession
    .builder()
    .appName("Passing Function to Spark")
    .config("spark.some.config.option", "some-value")
    .master("local")    //this is missing in the https://spark.apache.org/docs/latest/sql-programming-guide.html#upgrading-from-spark-sql-20-to-21 example.
    //But required to make it run correctly.
    //You need to specify master URL while launching a spark application via spark-submit. You can either add --master local[*] as a command line argument
    .getOrCreate()

  // For implicit conversions from RDDs to DataFrames
  import mySparkSession.implicits._

  case class Record(ip: String, port: String, access_dt: String, method: String, path:String, protocol:String)

  val accessLogDf: DataFrame = mySparkSession.sparkContext
    .textFile("in/web_access_log.txt")
    .map(_.split(" "))
    .map(attributes => Record(attributes(0), attributes(8), (attributes(3)+ attributes(4)).stripPrefix("[").stripSuffix("]"), attributes(5).stripPrefix("\""), attributes(6), attributes(7).stripSuffix("\"")))
    .toDF()

  accessLogDf.show(5, false)
  accessLogDf.createOrReplaceTempView("web_access_log")
  accessLogDf.printSchema()

}