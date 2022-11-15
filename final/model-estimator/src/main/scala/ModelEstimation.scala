import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => sf}
import scopt.OParser

import java.util.concurrent.TimeUnit

object ModelEstimation {
  private val APP_NAME = "ModelEstimation"

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val sparkConf = new SparkConf()
      .setAppName(APP_NAME)
      // Run Spark in local mode for the sake of simplicity
      .set("spark.master", "local[2]")

    val spark = SparkSession
      .builder
      .appName(APP_NAME)
      .config(sparkConf)
      .getOrCreate()


    val dfRaw = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", runtimeConfig.get.kafkaEndpoint)
      .option("subscribe", runtimeConfig.get.kafkaInputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // Describe input CSV schema
    val schema = StructType(
      Array(
        StructField("Timestamp", StringType),
        StructField("Occupancy", IntegerType),
        StructField("Prediction", IntegerType),
      )
    )

    // ..and parse CSV messages
    val input_col = sf.from_json(
      sf.col("value").cast("string"), schema
    )

    val colTs = sf.col("Timestamp")

    val df0 = dfRaw.withColumn("input", input_col).select("input.*")
    val dfTs = df0.select(sf.col("*"), sf.to_timestamp(colTs).as("ts"))
    val df = dfTs
      .drop("Timestamp").withColumnRenamed("ts", "Timestamp")
      .select(
        colTs,
        sf.col("Occupancy"),
        sf.col("Prediction"),
        sf.year(colTs).as("year"),
        sf.month(colTs).as("month"),
        sf.dayofmonth(colTs).as("day")
      )

//    df.writeStream
//      .format("console")
//      .outputMode("append")
//      .trigger(Trigger.ProcessingTime(runtimeConfig.get.triggerPeriod, TimeUnit.SECONDS))
//      .start()
//      .awaitTermination()

    val query = df.writeStream
      .outputMode(OutputMode.Append())
      .format("parquet")
      .partitionBy("year", "month", "day")
      .option("path", runtimeConfig.get.outputPath)
      .option("checkpointLocation", if (runtimeConfig.get.checkpointLocation == null) {
        runtimeConfig.get.outputPath + "/.checkpoint"
      } else {
        runtimeConfig.get.checkpointLocation
      })
      .trigger(Trigger.ProcessingTime(runtimeConfig.get.triggerPeriod, TimeUnit.SECONDS))
      .start()

    query.awaitTermination()
  }

  case class RuntimeConfig(
                            kafkaEndpoint: String = "localhost:29092",
                            kafkaGroupId: String = "group1",
                            kafkaInputTopic: String = null,
                            outputPath: String = null,
                            checkpointLocation: String = null,
                            triggerPeriod: Int = Predef.Integer2int(null)
                          )

  def parseCmdLine(args: Array[String]): Option[RuntimeConfig] = {
    val builder = OParser.builder[RuntimeConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName(APP_NAME),
        head("app", "0.1"),
        opt[String]("kafka-endpoint")
          .action((x, c) => c.copy(kafkaEndpoint = x))
          .optional()
          .text("Kafka broker endpoint (optional)"),
        opt[String]("kafka-group-id")
          .action((x, c) => c.copy(kafkaGroupId = x))
          .optional()
          .text("Group id (optional, default group1)"),
        opt[String]("kafka-input-topic")
          .action((x, c) => c.copy(kafkaInputTopic = x))
          .required()
          .text("Input Kafka topic (required)"),
        opt[String]("output-path")
          .action((x, c) => c.copy(outputPath = x))
          .required()
          .text("Output files path (required)"),
        opt[String]("checkpoint-location")
          .action((x, c) => c.copy(checkpointLocation = x))
          .optional()
          .text("Spark checkpoint location (optional)"),
        opt[String]("trigger-period")
          .action((x, c) => c.copy(triggerPeriod = x.toInt))
          .required()
          .text("Data spooling period in seconds (required)"),
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }
}
