package com.sparkTutorial.pairRdd.aggregation.reducebykey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd: RDD[String] = lines.flatMap(line => line.split(" "))
    val wordPairRdd: RDD[(String, Int)] = wordRdd.map(word => (word, 1))

    val wordCounts: RDD[(String, Int)] = wordPairRdd.reduceByKey((x, y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)
  }
}
