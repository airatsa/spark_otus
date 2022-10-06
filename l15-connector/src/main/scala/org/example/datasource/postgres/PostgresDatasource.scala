package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{DriverManager, ResultSet}
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Using


class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = new PostgresTable(properties.get("tableName")) // TODO: Error handling
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType()
    .add("user_id", LongType)
    .add("user_name", StringType)
}

case class ConnectionProperties(
                                 url: String, user: String, password: String, tableName: String,
                                 partitionColumn: String, partitionSize: Int) {
  if (partitionColumn.isEmpty && partitionSize > 0) {
    throw new IllegalArgumentException("Partition size was set but partitioning column was not defined")
  }
}

object ConnectionProperties {
  def apply(url: String, user: String, password: String, tableName: String): ConnectionProperties = {
    ConnectionProperties(url, user, password, tableName, "", 0)
  }
}

/** Read */

class PostgresScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = {
    new PostgresScan(ConnectionProperties(
      options.get("url"), options.get("user"), options.get("password"), options.get("tableName"),
      options.getOrDefault("partitioningColumn", ""),
      options.getOrDefault("partitionSize", "0").toInt
    ))
  }
}

class PostgresPartition(val valFrom: Int, val valTo: Int) extends InputPartition {
  def this() {
    this(-1, -1)
  }

  def isEmpty: Boolean = {
    valFrom < 0
  }
}

class PostgresScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    if (connectionProperties.partitionSize <= 0) {
      return Array(new PostgresPartition)
    }

    Using.resource(DriverManager.getConnection(
      connectionProperties.url, connectionProperties.user, connectionProperties.password
    )) {
      connection =>
        val statement = connection.createStatement()
        val partCol = connectionProperties.partitionColumn
        val resultSet = statement.executeQuery(
          s"select $partCol from ${connectionProperties.tableName} order by $partCol asc")

        class RsIterator(rs: ResultSet) extends Iterator[ResultSet] {
          def hasNext: Boolean = rs.next()

          def next(): ResultSet = rs
        }
        val values = new RsIterator(resultSet).map(_.getInt(1)).toArray

        val numVals = values.length
        val partSize = connectionProperties.partitionSize
        val numParts = (numVals + partSize - 1) / partSize

        (0 until numParts).map(i => {
          val idxFrom = i * partSize
          val idxTo = if (i == numParts - 1) numVals - 1 else (i + 1) * partSize - 1
          new PostgresPartition(values(idxFrom), values(idxTo)).asInstanceOf[InputPartition]
        }).toArray
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new PostgresPartitionReaderFactory(connectionProperties)
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PostgresPartitionReader(connectionProperties, partition.asInstanceOf[PostgresPartition])
  }
}

class PostgresPartitionReader(connectionProperties: ConnectionProperties, partition: PostgresPartition) extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    connectionProperties.url, connectionProperties.user, connectionProperties.password
  )
  private val statement = connection.createStatement()

  private val resultSet = statement.executeQuery(buildQuery())

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(
    resultSet.getLong(1),
    UTF8String.fromString(resultSet.getString(2))
  )

  override def close(): Unit = connection.close()

  def buildQuery(): String = {
    val queryBase = s"select * from ${connectionProperties.tableName}"
    if (partition.isEmpty) {
      return queryBase
    }
    val partCol = connectionProperties.partitionColumn
    queryBase + s" where $partCol >= ${partition.valFrom} and $partCol <= ${partition.valTo}"
  }
}

/** Write */

class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName")
  ))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  private val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  private val statement = "insert into users (user_id, user_name) values (?, ?)"
  private val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.setString(2, record.getString(1))
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}

