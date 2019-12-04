package com.sparkTutorial.LDpractice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RddUnion extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val mySparkSession: SparkSession = SparkSession
    .builder()
    .appName("Rdd Union")
    .config("spark.some.config.option", "some-value")
    .master("local")    //this is missing in the https://spark.apache.org/docs/latest/sql-programming-guide.html#upgrading-from-spark-sql-20-to-21 example.
    //But required to make it run correctly.
    //You need to specify master URL while launching a spark application via spark-submit. You can either add --master local[*] as a command line argument
    .getOrCreate()

  //spark session creates sparkConext too. Underlying sparkcontext can be fetched as follows
  val sc = mySparkSession.sparkContext

  val myLst: List[Int] = List(1, 2, 3, 3)
  val myRdd: RDD[Int] = sc.parallelize(myLst)

  myRdd.map(x => x +1).foreach(print)
  println()
  myRdd.flatMap(x => x.to(3)).foreach(print)
  println()
  myRdd.filter(x => x != 1).foreach(print)
  println()
  myRdd.distinct().foreach(print)
  println()
  val a: Int = 5
  print(a.to(10))
}
