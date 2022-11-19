package homework3

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import defs.Defs.PATH_TAXI_FACTS
import helper.Helper.{readFromDB, readTaxiFacts}
import homework3.DataApiHomeWorkTaxi.{BuildMetricsDF, calculateMetrics, saveDataMart}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Row, functions => sf}

import java.nio.file.{Files, Paths}
import java.sql.DriverManager


class TestHomework3 extends SharedSparkSession with ForAllTestContainer {

  override val container: PostgreSQLContainer = PostgreSQLContainer()

  test("Test taxi data mart") {
    Class.forName(container.driverClassName)


    val taxiFactsDF = readTaxiFacts(PATH_TAXI_FACTS)

    val metricsDF = BuildMetricsDF(calculateMetrics(taxiFactsDF))

    // Round to 3 decimal places to facilitate comparison
    val dfActual = metricsDF.select(sf.col("metric"), sf.round(sf.col("value"), 3).as("value"))

    checkAnswer(
      dfActual,
      Row("total_rides", 331893.0) ::
        Row("dist_min", 0.0) ::
        Row("dist_max", 66.0) ::
        Row("dist_average", 2.718) ::
        Row("dist_std_dev", 3.485)
        :: Nil
    )

    val numMartRows = dfActual.count()

    val connection = DriverManager.getConnection(
      container.jdbcUrl,
      container.username, container.password
    )

    val initQuery = new String(Files.readAllBytes(Paths.get("sql/homework3.sql")))

    val createTableStatement = connection.prepareStatement(initQuery)
    createTableStatement.execute()

    val jdbcOptions = Map[String, String](
      "driver" -> container.driverClassName,
      "url" -> container.jdbcUrl,
      "user" -> container.username,
      "password" -> container.password
    )

    saveDataMart(metricsDF, jdbcOptions)

    {
      val row = readFromDB(jdbcOptions, "select max(id) from rides_datamart").first()
      val id = row.getInt(0)
      assert(id == 0)
    }
    {
      val row = readFromDB(jdbcOptions, "select count(*) from rides_datamart").first()
      val num = row.getLong(0)
      assert(num == numMartRows)
    }

    saveDataMart(metricsDF, jdbcOptions)

    {
      val row = readFromDB(jdbcOptions, "select max(id) from rides_datamart").first()
      val id = row.getInt(0)
      assert(id == 1)
    }
    {
      val row = readFromDB(jdbcOptions, "select count(*) from rides_datamart").first()
      val num = row.getLong(0)
      assert(num == numMartRows * 2)
    }
  }

}

