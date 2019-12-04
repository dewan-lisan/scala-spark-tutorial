package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */


    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("LD_airports").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val airports: RDD[String] = sc.textFile("in/airports.text")
    val airportsInBD = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"Bangladesh\"")

    //This is how RDDs' can be printed. Showing all airport rows in country Bangladesh
    airportsInBD.collect().foreach(println) //this is not good if RDD has millons or rows
    airportsInBD.take(5).foreach(println)

    val airportNameCity: RDD[String] = airportsInBD.map(line => {
      val columns: Array[String] = line.split(Utils.COMMA_DELIMITER)
      columns(1) + " -- " + columns(2)
    }
    )

    airportNameCity.collect().foreach(println)
    airportNameCity.saveAsTextFile("out/airports_in_bangladesh.text")

  }
}
