package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */
    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD: RDD[String] = sc.textFile("in/airports.text") //regular RDD

    //PairRDD, contains tuple i.e. (key, value) pair
    val airportPairRDD: RDD[(String, String)] = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(0), line.split(Utils.COMMA_DELIMITER)(3)))
    val nonUsAirport = airportPairRDD.filter(keyvalue => keyvalue._2 != "\"United States\"")
    nonUsAirport.saveAsTextFile("out/airports_not_in_USA_pariRDD.txt")
  }
}
