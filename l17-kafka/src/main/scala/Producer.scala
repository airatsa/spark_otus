import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, writeToString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.io.{FileReader, Reader}
import java.util.Properties

object Producer extends App {
  val TOPIC_NAME = "books"

  val data = readBooksFromCsv("bestsellers with categories.csv")

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("acks", "all")

  val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

  implicit val codec: JsonValueCodec[BookInfo] = JsonCodecMaker.make
  data.foreach { r =>
    producer.send(new ProducerRecord(TOPIC_NAME, r.Name, writeToString(r)))
  }

  producer.close()

  case class BookInfo(
                       Name: String,
                       Author: String,
                       UserRating: Double,
                       Reviews: Int,
                       Price: Double,
                       Year: Int,
                       Genre: String
                     )

  // Reads csv file via Apache Commons CSV library
  def readBooksFromCsv(file: String): Seq[BookInfo] = {
    import org.apache.commons.csv._

    import scala.jdk.CollectionConverters._
    var reader: Reader = null
    try {
      reader = new FileReader(file)
      val parser = new CSVParser(
        new FileReader(file),
        CSVFormat.Builder.create()
          .setHeader("Name", "Author", "User Rating", "Reviews", "Price", "Year", "Genre")
          .setSkipHeaderRecord(true).setDelimiter(',').build()
      )
      parser.iterator().asScala.map { record =>
        BookInfo(
          record.get(0),
          record.get(1),
          record.get(2).toDouble,
          record.get(3).toInt,
          record.get(4).toDouble,
          record.get(5).toInt,
          record.get(6)
        )
      }.toSeq
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }
}

