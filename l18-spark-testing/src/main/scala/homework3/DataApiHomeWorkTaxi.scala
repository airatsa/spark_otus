package homework3

import defs.Defs.PATH_TAXI_FACTS
import helper.Helper.{readFromDB, readTaxiFacts, writeToDB}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => sf}

object DataApiHomeWorkTaxi {

  def calculateMetrics(taxiFactsDF: DataFrame): DataFrame = {
    val distCol = taxiFactsDF("trip_distance")
    val df = taxiFactsDF
      .select(distCol)
      .agg(
        sf.count(distCol).cast(DoubleType).as("total_rides"),
        sf.min(distCol).as("dist_min"),
        sf.max(distCol).as("dist_max"),
        sf.mean(distCol).as("dist_average"),
        sf.stddev(distCol).as("dist_std_dev")
      )
    df
  }

  def BuildMetricsDF(df: DataFrame): DataFrame = {
    val columns = df.columns
    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df1 = df
      .selectExpr("stack(" + columns.length + "," + stackCols + ")")
      .select(sf.col("col0").as("metric"), sf.col("col1").as("value"))
    df1
  }

  def saveDataMart(metricsDF: DataFrame, jdbcOptions: Map[String, String])(implicit spark: SparkSession): Unit = {
    // Detect data mart sample id. Each set of metrics (data mart sample) is written with the unique id
    val row =
      readFromDB(jdbcOptions, "select max(id) from rides_datamart").first()

    val nextDataMartId = if (row.isNullAt(0)) 0 else row.getInt(0) + 1

    val df = metricsDF
      .select(sf.lit(nextDataMartId).as("id"), sf.col("*"))

    writeToDB(df, jdbcOptions, "rides_datamart", SaveMode.Append)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    // Read fact files
    val taxiFactsDF = readTaxiFacts(PATH_TAXI_FACTS)

    // Calculate metrics
    val df = calculateMetrics(taxiFactsDF)
    df.show()

    // Build dataframe to write to JDBC table
    val metricsDF = BuildMetricsDF(df)
    metricsDF.show()

    // Write to JDBC table
    val jdbcOptions = Map[String, String](
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/homework3",
      "user" -> "docker",
      "password" -> "docker"
    )
    saveDataMart(metricsDF, jdbcOptions)
  }
}
