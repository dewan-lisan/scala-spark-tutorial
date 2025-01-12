package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports: RDD[String] = sc.textFile("in/airports.text")
    val airportsInUSA: RDD[String] = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat > 40)

    val airportsNameAndCityNames: RDD[String] = airportsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text")
  }
}
