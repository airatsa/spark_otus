package helper

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Helper {
  def readTaxiZones(path: String)(implicit spark: SparkSession): DataFrame = {
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
    val taxiZonesDF = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schemaTaxiZones)
      .load(path)

    taxiZonesDF
  }

  def readTaxiFacts(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.load(path)
  }

  def saveToParquet(df: DataFrame, path: String, saveMode: SaveMode): Unit = {
    df.coalesce(1)
      .write
      .mode(saveMode)
      .parquet(path)
  }

  def readFromDB(jdbcOptions: Map[String, String], query: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .options(jdbcOptions)
      .option("query", query)
      .load()
  }

    def writeToDB(df: DataFrame, jdbcOptions: Map[String, String], table: String, saveMode: SaveMode): Unit = {
      df.write
        .format("jdbc")
        .mode(saveMode)
        .options(jdbcOptions)
        .option("dbtable", table)
        .save()
    }
}
