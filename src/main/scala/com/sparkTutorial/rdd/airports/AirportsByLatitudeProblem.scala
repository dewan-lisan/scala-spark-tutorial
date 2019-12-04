package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */
    val conf = new SparkConf().setAppName("LD_airportsLattitude").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val airports: RDD[String] = sc.textFile("in/airports.text")
    //filtering airport whose latitude are bigger than 40
    val airports40latitude: RDD[String] = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toDouble > 40 )
    //airports40latitude.take(5).foreach(println)

    val airportNameLatitue = airports40latitude.map( line => {
      val columns = line.split(Utils.COMMA_DELIMITER)
      columns(0) + ", "+ columns(1)+ ", "+columns(6)
    }
    )
    //airportNameLatitue.take(1).foreach(println)
    airportNameLatitue.saveAsTextFile("out/airports_by_latitude.text")

  }
}
