from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, split, to_timestamp, unix_timestamp, when, window, count, abs as abs_func,  sum as pyspark_sum, date_format
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)
from datetime import datetime, timedelta
import socket

def main(args):
    if len(args) != 9:
        print("You need to provide 9 parameters: D, N, delay, bootstrapServer, jdbcUser, jdbcPassword, jdbcDatabase, anomaliesTopic, airportsFilePath")
        return

    D = int(args[0])
    N = int(args[1])
    delay = args[2]
    bootstrapServer = args[3]

    host_name = socket.gethostname()

    jdbc_user = args[4]
    jdbc_password = args[5]
    jdbcDatabase = args[6]
    jdbc_url = f"jdbc:postgresql://{host_name}:8432/delays_db"

    fileInput = args[8]
    anomaliesTopic = args[7] 

    spark = SparkSession.builder \
        .appName("FlightDelaysApp") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Set the configuration to prevent truncation
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)  

    airportSchema = StructType([
        StructField("airportID", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("IATA", StringType()),
        StructField("ICAO", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("altitude", IntegerType()),
        StructField("timezone", IntegerType()),
        StructField("DST", StringType()),
        StructField("timezoneName", StringType()),
        StructField("type", StringType()),
        StructField("state", StringType())
    ])

    flightsKafkaStream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServer) \
        .option("subscribe", "flights-input") \
        .option("startingOffsets", "latest") \
        .option("includeHeaders", "false") \
        .load()

    airportsDF = spark.read \
        .schema(airportSchema) \
        .option("delimiter", ",") \
        .csv(fileInput)

    airportsDF.show(10)

    flights = flightsKafkaStream.select(expr("CAST(value AS STRING)").alias("value"))

    splitCols = split(col("value"), ",")

    flightsDF = flights \
        .withColumn("airline", splitCols.getItem(0).cast(StringType())) \
        .withColumn("flightNumber", splitCols.getItem(1).cast(IntegerType())) \
        .withColumn("tailNumber", splitCols.getItem(2).cast(StringType())) \
        .withColumn("startAirport", splitCols.getItem(3).cast(StringType())) \
        .withColumn("destAirport", splitCols.getItem(4).cast(StringType())) \
        .withColumn("scheduledDepartureTime", to_timestamp(splitCols.getItem(5))) \
        .withColumn("scheduledDepartureDayOfWeek", splitCols.getItem(6).cast(IntegerType())) \
        .withColumn("scheduledFlightTime", splitCols.getItem(7).cast(IntegerType())) \
        .withColumn("scheduledArrivalTime", to_timestamp(splitCols.getItem(8))) \
        .withColumn("departureTime", to_timestamp(splitCols.getItem(9))) \
        .withColumn("taxiOut", splitCols.getItem(10).cast(IntegerType())) \
        .withColumn("distance", splitCols.getItem(11).cast(IntegerType())) \
        .withColumn("taxiIn", splitCols.getItem(12).cast(IntegerType())) \
        .withColumn("arrivalTime", to_timestamp(splitCols.getItem(13))) \
        .withColumn("diverted", splitCols.getItem(14)) \
        .withColumn("cancelled", splitCols.getItem(15)) \
        .withColumn("cancellationReason", splitCols.getItem(16)) \
        .withColumn("cancelationTime", to_timestamp(splitCols.getItem(22))) \
        .withColumn("orderColumn", to_timestamp(splitCols.getItem(23))) \
        .withColumn("infoType", splitCols.getItem(24).cast(StringType())) \
        .withColumn("departureDelay", \
            when(col("infoType") == 'D', (abs_func(unix_timestamp("departureTime") - unix_timestamp("scheduledDepartureTime"))).cast("integer")).otherwise(0)) \
        .withColumn("arrivalDelay", \
            when(col("infoType") == 'A', (abs_func(unix_timestamp("arrivalTime") - unix_timestamp("scheduledArrivalTime"))).cast("integer")).otherwise(0)) \
 
    watermarkedFlightsDF = flightsDF.withWatermark("orderColumn", "1 day")

    joinedDest = watermarkedFlightsDF.join(
        airportsDF,
        (watermarkedFlightsDF["startAirport"] == airportsDF["IATA"]) |
        (watermarkedFlightsDF["destAirport"] == airportsDF["IATA"]),
        "left"
    ).select("*")

    aggregatedFlightStats = joinedDest \
        .groupBy(window("orderColumn", "1 day"), "state") \
        .agg(
            count(when(col("infoType") == 'D', True)).alias("departure_count"),
            pyspark_sum("departureDelay").alias("total_departure_delay"),
            count(when(col("infoType") == 'A', True)).alias("arrival_count"),
            pyspark_sum("arrivalDelay").alias("total_arrival_delay")
        ) \
        .withColumn("day", col("window.start").cast("timestamp")) \
        .drop("window")

    #TODO finally change to 10 min
    sliding_window_size_minutes = "30 seconds" #"10 minutes"
    onlyDelays = flightsDF \
        .select(col("arrivalDelay"), col("departureDelay"))


    watermarkedFlightsAnomaliesDF = joinedDest.withWatermark("orderColumn", "5 minutes")

    anomaliesDF = watermarkedFlightsAnomaliesDF \
        .groupBy(window("orderColumn", sliding_window_size_minutes, "1 minute"), "destAirport")

    incoming_flights = watermarkedFlightsAnomaliesDF \
        .filter(col("infoType") == "A") \
        .select("IATA", "name", "city", "orderColumn", "scheduledArrivalTime", "flightNumber", "destAirport") \
        .withColumn("timeToArrive", abs_func(unix_timestamp("orderColumn") - unix_timestamp("scheduledArrivalTime")).cast("integer") / 60.0) \
        .groupBy(window("orderColumn", "10 minutes", "5 minutes"), "destAirport") \
        .agg(
            count(when((col("timeToArrive") > 30) & (col("timeToArrive") <= 30 + D), 
                    col("flightNumber"))).alias("filteredIncomingFlightsCount"),
            count("flightNumber").alias("totalIncomingFlightsCount")
        ) \
        .filter(col("filteredIncomingFlightsCount") >= N)

    airportForAnomaliesDF = airportsDF.select("name", "IATA", "city", "state")

    anomaliesFullData = incoming_flights.join(
        airportForAnomaliesDF,
        (incoming_flights["destAirport"] == airportForAnomaliesDF["IATA"]),
        "left"
    )

    #DELAYS 
    query = aggregatedFlightStats.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='30 seconds') \
            .start()

    jdbcProperties = {
        "user": jdbc_user,
        "password": jdbc_password,
        "driver": "org.postgresql.Driver"
    }

    if delay == "A":
        aggregatedDelaysQuery = aggregatedFlightStats \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(lambda batchDF, _: 
                batchDF
                .write
                .mode("overwrite")
                .jdbc(jdbc_url, 
                table="delays", 
                properties=jdbcProperties)) \
            .option("checkpointLocation", "/tmp/checkpoints/aggregatedDelays") \
            .trigger(processingTime="30 seconds") \
            .start()

    elif delay == "C":
        aggregatedDelaysQuery = aggregatedFlightStats \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(lambda batchDF, _: 
                batchDF
                .write
                .mode("append")
                .jdbc(jdbc_url, 
                table="delays", 
                properties=jdbcProperties)) \
            .option("checkpointLocation", "/tmp/checkpoints/aggregatedDelays")  \
            .trigger(processingTime="1 minute") \
            .start()

    #ANOMALIES

    query = anomaliesFullData \
            .selectExpr("CAST(IATA AS STRING) AS key", "to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrapServer) \
            .option("topic", anomaliesTopic) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/checkpoint/anomalies") \
            .trigger(processingTime='30 seconds') \
            .start()
    spark.streams.awaitAnyTermination()


def isCorrectInfoType(infoType):
    return infoType in ["A", "D"]

def parseOrderColumns(orderColumn):
    formatter = "%Y-%m-%d %H:%M:%S"
    local_date = datetime.strptime(orderColumn, formatter)
    return local_date.strftime("%Y-%m-%d")

if __name__ == "__main__":
    import sys
    from datetime import datetime
    main(sys.argv[1:])