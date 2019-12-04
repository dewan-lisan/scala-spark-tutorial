package com.sparkTutorial.rdd.count

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd: RDD[String] = sc.parallelize(inputWords)
    println("Count: " + wordRdd.count())    //count() is applied on RDD

    val wordCountByValue: collection.Map[String, Long] = wordRdd.countByValue()  //countByValue() is applied on Map
    println("CountByValue:")

    for ((word, count) <- wordCountByValue) println(word + " : " + count)

    /*Another way of implementation */
    // Transform into word and count.
    val counts: RDD[(String, Int)] = wordRdd.map(word => (word, 1))
      .reduceByKey( _ + _ )
    //.reduceByKey{case (x, y) => x + y }  //this is same as reduceByKey((x, y) => x + y ) or reduceByKey((x, y) => x + y )
    // Save the word count back out to a text file, causing evaluation.
    counts.collect().foreach(println)
    //counts.saveAsTextFile("out/CountExample.txt")
  }
}
