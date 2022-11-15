import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions => sf}
import scopt.OParser

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ModelTrainer {

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val date = if (runtimeConfig.get.date == null) {
      null
    } else {
      val format = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      LocalDate.parse(runtimeConfig.get.date, format)
    }

    val APP_NAME = "ModelTrainer"

    val sparkConf = new SparkConf()
      .setAppName("StreamingClassifier")
      // Run Spark in local mode for the sake of simplicity
      .set("spark.master", "local[2]")

    val spark = SparkSession
      .builder
      .appName("StructuredStreamingClassifier")
      .config(sparkConf)
      .getOrCreate()

    // Load data to train the model

    val df = if (date == null) {
      // Input data as a CSV file for bootstrap model training
      val df0 = spark.read.format("csv")
        .option("delimiter", ",")
        .option("header", "true")
        .schema(CSV_SCHEMA)
        .load(runtimeConfig.get.inputData)
      val dfTs = df0.select(sf.col("*"), sf.to_timestamp(sf.col("date"), "yyyy-MM-dd HH:mm:ss").as("timestamp"))
      val colTs = sf.col("timestamp")
      dfTs.select(
        sf.col("*"),
        sf.year(colTs).as("year"),
        sf.month(colTs).as("month"),
        sf.dayofmonth(colTs).as("day"),
        sf.hour(colTs).as("hour"),
        sf.minute(colTs).as("minute"),
        sf.second(colTs).as("second")
      ).drop("date, timestamp")
    } else {
      val year = date.getYear
      val month = date.getMonthValue
      val day = date.getDayOfMonth
      val path = s"${runtimeConfig.get.inputData}/year=$year/month=$month/day=$day"
      val df0 = spark.read.format("json")
        .schema(JSON_SCHEMA)
        .load(path)
      df0.withColumn("year", sf.lit(year))
        .withColumn("month", sf.lit(month))
        .withColumn("day", sf.lit(day))
    }

    df.show(10, truncate = false)

    // Train the model
    val featureColumns = Array("year", "month", "day", "hour", "minute", "Temperature", "Humidity", "Light", "CO2", "HumidityRatio")
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    val classifier = new DecisionTreeClassifier()
      .setLabelCol("Occupancy")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler, classifier))
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val pipelineModel = pipeline.fit(trainingData)
    pipelineModel.write.overwrite().save(runtimeConfig.get.output)
  }


  case class RuntimeConfig(
                            inputData: String = null,
                            date: String = null,
                            output: String = null
                          )

  def parseCmdLine(args: Array[String]): Option[RuntimeConfig] = {
    val builder = OParser.builder[RuntimeConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("StreamingClassifier"),
        head("app", "0.1"),
        opt[String]("input-data")
          .action((x, c) => c.copy(inputData = x))
          .required()
          .text("Input data location: CSV file of a folder with JSON files (required)"),
        opt[String]("date")
          .action((x, c) => c.copy(date = x))
          .optional()
          .text("Date of the data to train on (optional). input-data argument should point to a folder with JSON files"),
        opt[String]("output")
          .action((x, c) => c.copy(output = x))
          .required()
          .text("Folder to put trained model in  (required)")
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }

  private val CSV_SCHEMA = StructType(
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

  private val JSON_SCHEMA = StructType(
    Array(
      StructField("Temperature", DoubleType),
      StructField("Humidity", DoubleType),
      StructField("Light", DoubleType),
      StructField("CO2", DoubleType),
      StructField("HumidityRatio", DoubleType),
      StructField("Occupancy", IntegerType),
      StructField("hour", IntegerType),
      StructField("minute", IntegerType),
      StructField("second", IntegerType)
    )
  )

}
