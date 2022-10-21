package StreamingClassifier

import Helper.KafkaSink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions => sf}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OParser

import java.util.Properties

object StreamingClassifier {

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

    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val sparkContext = streamingContext.sparkContext

    // Load ML model
    val model = PipelineModel.load(runtimeConfig.get.modelPath)

    // Configure Kafka access
    val props: Properties = new Properties()
    props.put("bootstrap.servers", runtimeConfig.get.kafkaEndpoint)

    val kafkaSink = sparkContext.broadcast(KafkaSink(props))

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> runtimeConfig.get.kafkaEndpoint,
      ConsumerConfig.GROUP_ID_CONFIG -> runtimeConfig.get.kafkaGroupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // Subscribe to Kafka topic
    val inputTopicSet = Set(runtimeConfig.get.kafkaInputTopic)
    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
    )

    // Read messages from Kafka
    val lines = messages
      .map(_.value)

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
    val input_col = sf.from_json(sf.col("raw_value").cast("string"), schema)

    // Define output columns of interest
    val out_cols = Seq(
      sf.col("sepal_length"),
      sf.col("sepal_width"),
      sf.col("petal_length"),
      sf.col("petal_width"),
      sf.col("predictedSpecies").as("predicted_species")
    )

    // Iterate over messages
    lines.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        import spark.implicits._

        // Convert RDD to DataFrame
        val df0 = rdd.toDF("raw_value")

        // Parse input string
        val df = df0.withColumn("input", input_col).select("input.*")

        // Make predictions
        val dfPred = model.transform(df)

        // Transform data for output
        val dfOut = dfPred
          .select(out_cols: _*)
          .select(
            sf.col("predicted_species").as("key"),
            // covert the output to a JSON string
            sf.to_json(sf.struct($"*")).as("value")
          )
        // Save to Kafka
        dfOut.foreach { row => kafkaSink.value.send(runtimeConfig.get.kafkaOutputTopic, row(0).toString, row(1).toString) }
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

  case class RuntimeConfig(
                            kafkaEndpoint: String = "localhost:29092",
                            kafkaGroupId: String = "group0",
                            kafkaInputTopic: String = null,
                            kafkaOutputTopic: String = null,
                            modelPath: String = null
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
          .text("Path to ML model (required)")
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }
}
