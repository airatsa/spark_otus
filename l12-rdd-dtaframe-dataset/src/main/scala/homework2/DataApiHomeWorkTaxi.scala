package homework2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import java.time.ZoneOffset

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  case class TaxiFact(
      VendorID: Int,
      tpep_pickup_datetime: Timestamp,
      tpep_dropoff_datetime: Timestamp,
      passenger_count: Int,
      trip_distance: Double,
      RatecodeID: Int,
      store_and_fwd_flag: String,
      PULocationID: Int,
      DOLocationID: Int,
      payment_type: Int,
      fare_amount: Double,
      extra: Double,
      mta_tax: Double,
      tip_amount: Double,
      tolls_amount: Double,
      improvement_surcharge: Double,
      total_amount: Double
  )

  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  import spark.implicits._

  val taxiFactsDS = taxiFactsDF.as[TaxiFact]

  val taxiFactsRDD: RDD[TaxiFact] = taxiFactsDS.rdd

  // Use 1-hour slots
  val finalRDD = taxiFactsRDD
    .map(f => (f.tpep_pickup_datetime.toInstant.atZone(ZoneOffset.UTC).getHour, 1))
    .reduceByKey(_ + _)
    .sortByKey()

  val finalDF = finalRDD.toDF("hour", "num_rides")

  finalDF.show(24)

  // Write to single file
  finalDF
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("out/popular_hours")

}
