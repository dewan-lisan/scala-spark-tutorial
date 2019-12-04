package com.sparkTutorial.pairRdd.mapValues

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AirportsUppercaseProblem {
  def testPrint(str1: String): Unit ={
    println(str1)
  }

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */
    val conf = new SparkConf().setAppName("airports").setMaster("local")
    val sc = new SparkContext(conf)

    val airportsRDD: RDD[String] = sc.textFile("in/airports.text") //regular RDD
    val airportPairRdd: RDD[(String, String)] = airportsRDD.map(line => (line.split(Utils.COMMA_DELIMITER)(1),
      line.split(Utils.COMMA_DELIMITER)(3)))    //converting RDD to PairRDD i.e. to (Key, Value) pair
    val canadianAirportsPairRDD: RDD[(String, String)] = airportPairRdd.filter(abc => abc._2 == "\"Canada\"") //what to see Canadian airports only
    val canadianAirpPairRddUpper: RDD[(String, String)] = canadianAirportsPairRDD.mapValues(x => x.toUpperCase) //mapValue works ONLY with values of (Kye, Value) pair
    canadianAirpPairRddUpper.saveAsTextFile("out/airport_mapValue_uppercase.txt")

  }
}
