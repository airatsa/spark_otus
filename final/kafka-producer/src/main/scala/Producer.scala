import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scopt.OParser

import java.util.Properties
import scala.io.Source
import scala.util.Using

object Producer {
  def main(args: Array[String]): Unit = {
    val runtimeConfig = parseCmdLine(args)
    if (runtimeConfig.isEmpty) {
      return
    }

    val props = new Properties()
    props.put("bootstrap.servers", runtimeConfig.get.kafkaEndpoint)
    props.put("acks", "all")
    val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

    // Read file line by line and send each line to Kafka
    var count  = 0
    Using(Source.fromFile(runtimeConfig.get.inputFile)) { source =>
      source.getLines().drop(1).foreach { line =>
        count += 1
        println(s"Sending records: $count")
        producer.send(new ProducerRecord(runtimeConfig.get.kafkaOutputTopic, count.toString, line))
        Thread.sleep(runtimeConfig.get.period)
      }
    }

    producer.close()
  }

  case class RuntimeConfig(
                            kafkaEndpoint: String = "localhost:29092",
                            kafkaOutputTopic: String = null,
                            inputFile: String = null,
                            period: Int = 1000)

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
        opt[String]("period")
          .action((x, c) => c.copy(period = x.toInt))
          .optional()
          .text("Period in milliseconds between messages")
      )
    }
    OParser.parse(parser, args, RuntimeConfig())
  }

}
