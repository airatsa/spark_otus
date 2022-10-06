package homework1

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, desc, first, min, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // Define CSV file schema...
  val schemaTaxiZones = StructType(
    Array(
      StructField("LocationID", IntegerType, nullable = true),
      StructField("Borough", StringType, nullable = true),
      StructField("Zone", StringType, nullable = true),
      StructField("service_zone", StringType, nullable = true)
    )
  )

  // And read it
  val taxiZonesDF = spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .schema(schemaTaxiZones)
    .load("src/main/resources/data/taxi_zones.csv")

  // Read facts files
  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  // Perform data processing
  val colPULocID = taxiFactsDF("PULocationID")
  val colLocID = taxiZonesDF("LocationID")
  val pickUpsDF = taxiFactsDF.select(colPULocID)
    .join(taxiZonesDF, colPULocID === colLocID)
    .drop(colPULocID)
    .groupBy(colLocID)
    .agg(
      count(colLocID).as("num_pickups"),
      first("Borough").as("Borough"),
      first("Zone").as("Zone"),
      first("service_zone").as("service_zone"),
    )
    .orderBy(desc("num_pickups"))

  // Validate results
  val numFactsRaw = taxiFactsDF.count()
  val numFacts = pickUpsDF.agg(sum("num_pickups")).first().get(0)
  println(numFactsRaw, numFacts, numFactsRaw == numFacts)

  // Print top 10 locations
  pickUpsDF.show(10)

  // Write to single file
  pickUpsDF.coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("out/popular_locs")
}
