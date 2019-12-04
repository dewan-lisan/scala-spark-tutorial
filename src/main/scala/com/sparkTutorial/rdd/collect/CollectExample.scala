package com.sparkTutorial.rdd.collect

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CollectExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("collect").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords: List[String] = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val wordRdd: RDD[String] = sc.parallelize(inputWords)  //this converts List to RDD

    val words: Array[String] = wordRdd.collect()

    for (word: String <- words) println(word)
  }
}
