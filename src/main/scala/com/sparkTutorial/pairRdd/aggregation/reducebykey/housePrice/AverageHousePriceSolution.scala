package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("in/RealEstate.csv")
    val cleanedLines: RDD[String] = lines.filter(line => !line.contains("Bedrooms"))  //filters the header

    val bedroom_type: RDD[String] = cleanedLines.map(x => x.split(",")(3))
    println("bedroom types array")
    bedroom_type.collect().foreach(println)

    val housePricePairRdd: RDD[(String, (Int, Double))] = cleanedLines.map(line => (line.split(",")(3), (1, line.split(",")(2).toDouble)))
    println("housePricePairRdd")
    housePricePairRdd.collect().foreach(println)

    //val housePriceTotalv2: RDD[(String, (Int, Double))] = housePricePairRdd.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)) //reduceByKey works with values only. Keys are internally maintained.
    // here lets say, a._1 represents first raw first value, b._1 represents 2nd row first value! This gives total number of 4 room apartment given 1 is the value in each raw
    //a._2 represents first raw 2nd value, b._2 represents 2nd raw 2nd value. This gives total price of 4 room apartment!

    val housePriceTotal: RDD[(String, (Int, Double))] = housePricePairRdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("housePriceTotal: ")
    for ((bedroom, total) <- housePriceTotal.collect()) println(bedroom + " : " + total)

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("housePriceAvg: ")
    for ((bedroom, avg) <- housePriceAvg.collect()) println(bedroom + " : " + avg)
  }
}
