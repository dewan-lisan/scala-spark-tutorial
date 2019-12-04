package com.sparkTutorial.LDpractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/*
This program reads CSV file using SparkSession API (instead of SparkContext which is deprecated),
loads data into dataframe,
then aggregates data using Spark SQL and writes the result into a single csv file
 */

object MyFunctions extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val mySparkSession: SparkSession = SparkSession
    .builder()
    .appName("UK_postcode_region_count")
    .config("spark.sql.codegen", "false") //for ad-hoc quries, should be false. For large repeatedly running queries, should be true
    .config("spark.sql.inMemoryColumnarStorage.batchSize", 1000) //batch size should be optimized according to data volume
    .master("local")    //this is missing in the https://spark.apache.org/docs/latest/sql-programming-guide.html#upgrading-from-spark-sql-20-to-21 example.
                        //But required to make it run correctly.
                        //You need to specify master URL while launching a spark application via spark-submit. You can either add --master local[*] as a command line argument
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  //import mySparkSession.implicits._

  def getFullName(firstName: String, lastName: String): String = firstName + ' ' + lastName
  def getUpperRegion(region: String): String = region.toUpperCase


  //val df: DataFrame = mySparkSession.read.csv("in/uk-postcode.csv") //regular DataFrame
  val df: DataFrame = mySparkSession.read
    .format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")    //dont know what this is
    .option("inferSchema", "true")
    .load("in/uk-postcode.csv")
  df.show(10)
  df.printSchema()
  df.createOrReplaceTempView("uk_postcode_regions")

  val resultPostCodeRegion: DataFrame = mySparkSession.sql("select upper(region) as Region, count(1) as num_of_region from Uk_postcode_regions group by region")
  resultPostCodeRegion.show()
  resultPostCodeRegion.coalesce(1).write
    .mode(SaveMode.Overwrite)
    .csv("out/UK_postcode_region_count.csv")

}
