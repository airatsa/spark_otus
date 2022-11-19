package homework1

import defs.Defs.{PATH_TAXI_FACTS, PATH_TAXI_ZONES}
import helper.Helper.{readTaxiFacts, readTaxiZones}
import homework1.DataApiHomeWorkTaxi.getPickupAresByPopularity
import jdk.tools.jlink.internal.TaskHelper
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Row, functions}


class TestHomework1 extends SharedSparkSession {

  test("Test data processing") {

    val taxiZonesDF = readTaxiZones(PATH_TAXI_ZONES)
    val taxiFactsDF = readTaxiFacts(PATH_TAXI_FACTS)
    // Perform data processing
    val pickUpsDF = getPickupAresByPopularity(taxiFactsDF, taxiZonesDF)

    // Validate results
    val numFactsRaw = taxiFactsDF.count()
    val numFacts = pickUpsDF.agg(functions.sum("num_pickups")).first().get(0)
    assert(numFactsRaw == numFacts)

    val allRows = pickUpsDF.collect()

    // Check that the first and the last row
    val rows = Seq(allRows(0), allRows(allRows.length - 1))

    val dfActual = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      pickUpsDF.schema
    )

    checkAnswer(
      dfActual,
      Row(237, 15945, "Manhattan", "Upper East Side South", "YELLOW") ::
        Row(251, 1, "Staten Island", "Westerleigh", "BORO") :: Nil
    )
  }

}

