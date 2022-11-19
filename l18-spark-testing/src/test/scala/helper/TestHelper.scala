package helper

import defs.Defs.PATH_TAXI_ZONES
import helper.Helper.readTaxiZones
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.test.SharedSparkSession


class TestHelper extends SharedSparkSession {

  test("Read Taxi zones datasource") {
    val df = readTaxiZones(PATH_TAXI_ZONES)

    val numRows = df.count()
    assert(numRows == 265)


    for (col <- df.columns) {
      // Check column has non-null values.
      // If all values are null, then the column is missing in the CSV file.
      assert(numNullValues(df, col) < numRows)
    }
  }

  private def numNullValues(df: DataFrame, colName: String): Long = {
    df.filter(df(colName).isNull).count()
  }

}

