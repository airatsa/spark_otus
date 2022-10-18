import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

object Consumer extends App {
  val TOPIC_NAME = "books"
  val ITEMS_PER_PARTITION = 5

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumer4")
  props.put("auto.offset.reset", "earliest")
  props.put("enable.auto.commit", "false")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  consumer.subscribe(List(TOPIC_NAME).asJava)

  // Get partition list
  val partitions = getPartitions(consumer)

  // Rewind all partitions to the end
  consumer.seekToEnd(partitions.asJava)

  // Generate a list of read tasks per partition
  // Each task contains a range of offsets to read from a particular partition
  val tasks = partitions.map(p => {
    val endPos = consumer.position(p)
    println(s"Partition $p: $endPos")
    val startPos = math.max(endPos - ITEMS_PER_PARTITION, 0)
    val numItems = endPos - startPos
    ReadTask(p, startPos, numItems)
  }
  )

  val totalItemsToRead = tasks.map(_.numItems).sum
  println(s"Total partitions: ${tasks.size}, total items to read: $totalItemsToRead")

  // Rewind partitions to the start position
  tasks.foreach(t => {
    consumer.seek(t.partition, t.startPos)
  })

  // Read records and collect them into a list
  var items = Seq[String]()
  while (items.size < totalItemsToRead) {
    val records = consumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      items :+= record.value()
    }
  }

  consumer.close()

  items.zipWithIndex.foreach(i => {
    println(s"${i._2 + 1}: ${i._1}")
  })

  case class ReadTask(partition: TopicPartition, startPos: Long, numItems: Long) {
  }

  private def getPartitions(consumer: Consumer[_, _]): Seq[TopicPartition] = {
    while (true) {
      consumer.poll(Duration.ofMillis(100))
      val partitions = consumer.assignment()
      if (!partitions.isEmpty) {
        return partitions.asScala.toSeq
      }
    }
    null
  }
}
