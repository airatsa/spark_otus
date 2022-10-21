package StructuredStreamingClassifier

import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions => sf}
import scopt.OParser

object StructuredStreamingClassifier {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val sparkConf = new SparkConf()
      .setAppName("StreamingClassifier")
      // Run Spark in local mode for the sake of simplicity
      .set("spark.master", "local[2]")

    val spark = SparkSession
      .builder
      .appName("StructuredStreamingClassifier")
      .config(sparkConf)
      .getOrCreate()


    // Load ML model
    val model = PipelineModel.load(runtimeConfig.get.modelPath)

    val df0 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", runtimeConfig.get.kafkaEndpoint)
      .option("subscribe", runtimeConfig.get.kafkaInputTopic)
      //.option("startingOffsets", "earliest")
      .load()

    // Strings in Kafka are in JSON format, so we need to parse them
    // Define Kafka message schema..
    val schema = StructType(
      Array(
        StructField("sepal_length", DoubleType),
        StructField("sepal_width", DoubleType),
        StructField("petal_length", DoubleType),
        StructField("petal_width", DoubleType)
      )
    )
    // ..and parse JSON messages
    val input_col = sf.from_json(sf.col("value").cast("string"), schema)

    // Define output columns of interest
    val out_cols = Seq(
      sf.col("sepal_length"),
      sf.col("sepal_width"),
      sf.col("petal_length"),
      sf.col("petal_width"),
      sf.col("predictedSpecies").as("predicted_species")
    )

    // Parse input string
    val df = df0.withColumn("input", input_col).select("input.*")

    // Make predictions
    val dfPred = model.transform(df)

    import spark.implicits._

    // Transform data for output
    val dfOut = dfPred
      .select(out_cols: _*)
      .select(
        sf.col("predicted_species").as("key"),
        // covert the output to a JSON string
        sf.to_json(sf.struct($"*")).as("value")
      )

    // Save to Kafka
    val query = dfOut.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", runtimeConfig.get.kafkaEndpoint)
      .option("topic", runtimeConfig.get.kafkaOutputTopic)
      .option("checkpointLocation", runtimeConfig.get.checkpointLocation)
      .start()

    query.awaitTermination()
  }

  case class RuntimeConfig(
                            kafkaEndpoint: String = "localhost:29092",
                            kafkaGroupId: String = "group1",
                            kafkaInputTopic: String = null,
                            kafkaOutputTopic: String = null,
                            modelPath: String = null,
                            checkpointLocation: String = ".spark-checkpoint"
                          )

  def parseCmdLine(args: Array[String]): Option[RuntimeConfig] = {
    val builder = OParser.builder[RuntimeConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("StreamingClassifier"),
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
        opt[String]("kafka-output-topic")
          .action((x, c) => c.copy(kafkaOutputTopic = x))
          .required()
          .text("Input Kafka topic (required)"),
        opt[String]("model-path")
          .action((x, c) => c.copy(modelPath = x))
          .required()
          .text("Path to ML model (required)"),
        opt[String]("checkpoint-location")
          .action((x, c) => c.copy(modelPath = x))
          .optional()
          .text("Spark checkpoint location (optional)"),
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }
}
