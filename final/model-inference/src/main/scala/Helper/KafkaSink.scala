package Helper

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer: KafkaProducer[String, String] = createProducer()

  def send(topic: String, key: String, value: String): Unit = {
    //println(topic, key, value)
    producer.send(new ProducerRecord(topic, key, value))
  }
}

object KafkaSink {
  def apply(props: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}
