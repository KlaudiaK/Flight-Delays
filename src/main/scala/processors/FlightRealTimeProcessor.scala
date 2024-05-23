package put.poznan.pl.klaudiak
package processors

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, window}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class FlightRealTimeProcessor {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("FlightDelaysApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val D: Int = args(0).toInt
    val N: Int = args(1).toInt
    val delay: String = args(2)
    val bootstrapServer: String = args(3) // "localhost:9092"

    val airportsInputTopic = "airports-input"
    val airportsOutputTopic = "airports-output"
    val flightsInputTopic = "flights-input"
    val flightsOutputTopic = "flights-output"
    /*
     */
    val flightSchema = StructType(
      Array(
        StructField("airline", StringType),
        StructField("flightNumber", StringType),
        StructField("tailNumber", StringType),
        StructField("startAirport", StringType),
        StructField("destAirport", StringType),
        StructField("scheduledDepartureTime", TimestampType),
        StructField("scheduledDepartureDayOfWeek", IntegerType),
        StructField("scheduledFlightTime", IntegerType),
        StructField("scheduledArrivalTime", TimestampType),
        StructField("departureTime", TimestampType),
        StructField("taxiOut", IntegerType),
        StructField("distance", IntegerType),
        StructField("taxiIn", IntegerType),
        StructField("arrivalTime", TimestampType),
        StructField("diverted", BooleanType),
        StructField("cancelled", BooleanType),
        StructField("cancellationReason", StringType),
        StructField("airSystemDelay", IntegerType),
        StructField("securityDelay", IntegerType),
        StructField("airlineDelay", IntegerType),
        StructField("lateAircraftDelay", IntegerType),
        StructField("weatherDelay", IntegerType),
        StructField("cancelationTime", TimestampType),
        StructField("orderColumn", TimestampType),
        StructField("infoType", StringType)
      )
    )

    val airportSchema = StructType(
      Array(
        StructField("airportID", IntegerType),
        StructField("name", StringType),
        StructField("city", StringType),
        StructField("country", StringType),
        StructField("IATA", StringType),
        StructField("ICAO", StringType),
        StructField("latitude", DoubleType),
        StructField("longitude", DoubleType),
        StructField("altitude", IntegerType),
        StructField("timezone", IntegerType),
        StructField("DST", StringType),
        StructField("timezoneName", StringType),
        StructField("type", StringType),
        StructField("state", StringType)
      )
    )

    val flightsKafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", flightsInputTopic)
      .load()

    val airportsKafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", airportsInputTopic)
      .load()

    val flights = flightsKafkaStream
      .select(from_json($"value".cast(StringType), flightSchema).as("flights"))
      .select("data.*")

    val airports = airportsKafkaStream
      .select(from_json($"value".cast(StringType), airportSchema).as("airports"))

/*
    val flightsWithAirports = flights.join(airports, $"flights.startAirport" === $"airports.IATA", "left")


    val flightDataRecords = flights
      .filter($"data.infoType" === "D" || $"data.infoType" === "A")
      .groupBy(
        window($"data.orderColumn", "1 day", "1 day"),

      )

 */

    flights
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
      .awaitTermination()
  }

  def isCorrectInfoType(infoType: String): Boolean = {
    infoType match {
      case "A" => true
      case "D" => true
      case _ => false
    }
  }

  def parseOrderColumns(orderColumn: String): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val localDate = LocalDate.parse(orderColumn, formatter)
    localDate.toString
  }
}
