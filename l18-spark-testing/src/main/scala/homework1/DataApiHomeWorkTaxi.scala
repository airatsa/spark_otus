package homework1

import defs.Defs.{PATH_TAXI_FACTS, PATH_TAXI_ZONES}
import helper.Helper.{readTaxiFacts, readTaxiZones, saveToParquet}
import org.apache.spark.sql.functions.{count, desc, first, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataApiHomeWorkTaxi {

  def getPickupAresByPopularity(
      taxiFactsDF: DataFrame,
      taxiZonesDF: DataFrame
  ): DataFrame = {
    val colPULocID = taxiFactsDF("PULocationID")
    val colLocID = taxiZonesDF("LocationID")
    val pickUpsDF = taxiFactsDF
      .select(colPULocID)
      .join(taxiZonesDF, colPULocID === colLocID)
      .drop(colPULocID)
      .groupBy(colLocID)
      .agg(
        count(colLocID).as("num_pickups"),
        first("Borough").as("Borough"),
        first("Zone").as("Zone"),
        first("service_zone").as("service_zone")
      )
      .orderBy(desc("num_pickups"))
    pickUpsDF
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiZonesDF =
      readTaxiZones(PATH_TAXI_ZONES)
    // Read facts files
    val taxiFactsDF =
      readTaxiFacts(PATH_TAXI_FACTS)
    // Perform data processing
    val pickUpsDF = getPickupAresByPopularity(taxiFactsDF, taxiZonesDF)

    // Validate results
    val numFactsRaw = taxiFactsDF.count()
    val numFacts = pickUpsDF.agg(sum("num_pickups")).first().get(0)
    println(numFactsRaw, numFacts, numFactsRaw == numFacts)

    // Print top 10 locations
    pickUpsDF.show(10, truncate = false)

    // Write to single file
    saveToParquet(pickUpsDF, "out/popular_locs", SaveMode.Overwrite)
  }

}
