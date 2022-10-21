package KafkaProducer

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, writeToString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scopt.OParser

import java.io.{FileReader, Reader}
import java.util.Properties
import scala.collection.JavaConverters.asScalaIteratorConverter

object Producer {
  def main(args: Array[String]): Unit = {
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }


    val data = readDataFromCsv(runtimeConfig.get.inputFile)

    val props = new Properties()
    props.put("bootstrap.servers", runtimeConfig.get.kafkaEndpoint)
    props.put("acks", "all")

    val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

    implicit val codec: JsonValueCodec[ObjectInfo] = JsonCodecMaker.make
    data.foreach { r =>
      producer.send(new ProducerRecord(runtimeConfig.get.kafkaOutputTopic, r.species, writeToString(r)))
    }

    producer.close()
  }

  case class ObjectInfo(
                         sepal_length: Double,
                         sepal_width: Double,
                         petal_length: Double,
                         petal_width: Double,
                         species: String
                       )

  // Reads csv file via Apache Commons CSV library
  def readDataFromCsv(file: String): Seq[ObjectInfo] = {
    var reader: Reader = null
    try {
      reader = new FileReader(file)
      val parser = new CSVParser(
        new FileReader(file),
        CSVFormat.Builder.create()
          .setHeader("sepal_length", "sepal_width", "petal_length", "petal_width", "species")
          .setSkipHeaderRecord(true).setDelimiter(',').build()
      )
      parser.iterator().asScala.map { record =>
        ObjectInfo(
          record.get(0).toDouble,
          record.get(1).toDouble,
          record.get(2).toDouble,
          record.get(3).toDouble,
          record.get(4)
        )
      }.toSeq
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  case class RuntimeConfig(
                            kafkaEndpoint: String = "localhost:29092",
                            kafkaOutputTopic: String = null,
                            inputFile: String = null)

  def parseCmdLine(args: Array[String]): Option[RuntimeConfig] = {
    val builder = OParser.builder[RuntimeConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("app"),
        head("app", "0.1"),
        opt[String]("kafka-endpoint")
          .action((x, c) => c.copy(kafkaEndpoint = x))
          .optional()
          .text("Kafka endpoint (optional)"),
        opt[String]("kafka-output-topic")
          .action((x, c) => c.copy(kafkaOutputTopic = x))
          .required()
          .text("Kafka output topic"),
        opt[String]("input-file")
          .action((x, c) => c.copy(inputFile = x))
          .required()
          .text("Input file"),
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }

}
