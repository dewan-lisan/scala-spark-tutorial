package com.sparkTutorial.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("in/word_count.text")
    lines.collect().foreach(println)
    val words: RDD[String] = lines.flatMap(line => line.split(" "))

    val wordCnt: collection.Map[String, Long] = words.countByValue()
    for ((wrd: String, cnt1: Long) <- wordCnt) println(wrd + " : " + cnt1)
  }
}
