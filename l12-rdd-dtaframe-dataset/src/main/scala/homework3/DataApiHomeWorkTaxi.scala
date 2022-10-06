package homework3

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions => sf}

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val jdbcOptions = Map[String, String](
    "driver"   -> "org.postgresql.Driver",
    "url"      -> "jdbc:postgresql://localhost:5432/homework3",
    "user"     -> "docker",
    "password" -> "docker"
  )

  // Read fact files
  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  // Calculate metrics
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
  df.show()

  // Build dataframe to write to JDBC table
  val metricsDF = BuildMetricsDF(df)
  metricsDF.show()

  // Detect data mart sample id. Each set of metrics (data mart sample) is written with the unique id
  val row = spark.read
    .format("jdbc")
    .options(jdbcOptions)
    .option("query", "select max(id) from rides_datamart")
    .load()
    .first()

  val nextDataMartId = if (row.isNullAt(0)) 0 else row.getInt(0) + 1

  metricsDF
    .select(sf.lit(nextDataMartId).as("id"), sf.col("*"))
    .write
    .format("jdbc")
    .mode(SaveMode.Append)
    .options(jdbcOptions)
    .option("dbtable", "rides_datamart")
    .save()

  def BuildMetricsDF(df: DataFrame): DataFrame = {
    val columns      = df.columns
    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols    = columnsValue.mkString(",")
    val df1 = df
      .selectExpr("stack(" + columns.length + "," + stackCols + ")")
      .select(sf.col("col0").as("metric"), sf.col("col1").as("value"))
    df1
  }
}
