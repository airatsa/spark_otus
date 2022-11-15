import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => sf}
import scopt.OParser

object StreamingInference {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val APP_NAME = "StreamingInference"

    val sparkConf = new SparkConf()
      .setAppName(APP_NAME)
      // Run Spark in local mode for the sake of simplicity
      .set("spark.master", "local[2]")

    val spark = SparkSession
      .builder
      .appName(APP_NAME)
      .config(sparkConf)
      .getOrCreate()


    // Load ML model
    val model = PipelineModel.load(runtimeConfig.get.modelPath)

    val dfRaw = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", runtimeConfig.get.kafkaEndpoint)
      .option("subscribe", runtimeConfig.get.kafkaInputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // Strings in Kafka are in CSV format, so we need to parse them
    // Define Kafka message schema..
    val schema = StructType(
      Array(
        StructField("date", StringType),
        StructField("Temperature", DoubleType),
        StructField("Humidity", DoubleType),
        StructField("Light", DoubleType),
        StructField("CO2", DoubleType),
        StructField("HumidityRatio", DoubleType),
        StructField("Occupancy", IntegerType)
      )
    )
    // ..and parse CSV messages
    val input_col = sf.from_csv(
      sf.col("value").cast("string"), schema, Map("header" -> "false", "sep" -> ",")
    )

    val df0 = dfRaw.withColumn("input", input_col).select("input.*")

    val dfTs = df0.select(sf.col("*"), sf.to_timestamp(sf.col("date"), "yyyy-MM-dd HH:mm:ss").as("timestamp"))
    val colTs = sf.col("timestamp")
    val df = dfTs.select(
      sf.col("*"),
      sf.year(colTs).as("year"),
      sf.month(colTs).as("month"),
      sf.dayofmonth(colTs).as("day"),
      sf.hour(colTs).as("hour"),
      sf.minute(colTs).as("minute"),
      sf.second(colTs).as("second")
    ).drop("date")

    // Make predictions
    val dfPred = model.transform(df)

    // Define output columns of interest
    val out_cols = Seq(
      sf.col("timestamp"),
      sf.col("Occupancy"),
      sf.col("prediction").cast("integer").as("prediction")
    )

    import spark.implicits._

    // Transform data for output
    val dfOut = dfPred
      .select(out_cols: _*)
      .select(
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

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        println("Shutdown process initiated")
        query.stop()
        query.awaitTermination()
      }
    })

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
