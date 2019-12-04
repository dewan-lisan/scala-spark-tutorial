import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

val spark = SparkSession
            .builder()
            .appName("SparkApp")
            .master("local")
            .getOrCreate()

import spark.implicits._

case class EventSchema(organizer: String, name: String, budget: Int)

val events: DataFrame = spark.sparkContext
            .textFile("in/airports.text")
  .map(_.split(";"))
  .map(attributes => EventSchema(attributes(0), attributes(1), attributes(2).toInt))
  .toDF()

events.createOrReplaceTempView("EventView")

val allEvents = spark.sql("select * from EventView limit 2")
allEvents.show(false)




