package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem extends App {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
  val sc = new SparkContext(conf)

  val lines: RDD[String] = sc.textFile("in/word_count.text")
  //using flatmap to split the paragraph into words
  val wordRdd: RDD[String] = lines.flatMap(line => line.split(" "))
  val wordPairRdd: RDD[(String, Int)] = wordRdd.map(word => (word, 1))

  val wordCounts: RDD[(String, Int)] = wordPairRdd.reduceByKey((x, y) => x + y)
  //for ((word, count) <- wordCounts.collect()) println(word + " : " + count)

  val sortedWordCounts: RDD[(String, Int)] = wordCounts.sortBy(x => x._2, ascending = false)
  for( (x, y) <- sortedWordCounts.collect() ) println(x + " : " + y)

}

