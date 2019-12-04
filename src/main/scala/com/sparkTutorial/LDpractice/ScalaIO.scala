package com.sparkTutorial.LDpractice

import java.io.{FileInputStream, IOException}

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source


object ScalaIO {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    /*println(Source.fromFile("in/RealEstate.csv"))
    val s1 = Source.fromFile("in/RealEstate.csv").mkString
    println(s1)*/
/*
    val ss = SparkSession.builder().appName("TestApp").master("local").getOrCreate()
    val df = ss.read.option("header", "true").csv("in/RealEstate.csv")
    df.createOrReplaceTempView("realestate")
    df.show(false)

    //avro schema attribute names can't contain . or blank spaces
    df.write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save("out/RealEstate.avro")

    println("Avro is generated")*/
    println("Reading avro....")

    var in = None: Option[FileInputStream]

    try{
      in = Some(new FileInputStream("out/RealEstate.avro/RealEstate_data.avro"))
      println(in.mkString)
    } catch {
      case e: IOException => e.printStackTrace
    } finally {
      println("Everything went fine...")
    }


  }

}
