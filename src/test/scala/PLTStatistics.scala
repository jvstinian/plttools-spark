/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
import com.jvstinian.rms.aggregationtools.PLT
import com.jvstinian.rms.aggregationtools.SimplePLTRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec

class PLTStatisticsSpec extends FixtureAnyFlatSpec {

  case class FixtureParam(spark: SparkSession, data: DataFrame)

  def withFixture(test: OneArgTest): Outcome = {
    val spark: SparkSession = SparkSession.builder().appName("PLTCalculatorTest").master("local").getOrCreate()
    val records: Seq[SimplePLTRecord] = Seq(
      SimplePLTRecord(1, 1.0 / 5.0, 3500016, "3/13/2016 12:00:00 AM", "03/10/2016 00:00", 100),
      SimplePLTRecord(3, 1.0 / 5.0, 3500129, "8/25/2016 12:00:00 AM", "8/24/2016 12:00:00 AM", 200),
      SimplePLTRecord(3, 1.0 / 5.0, 3500140, "01/01/2017 00:00", "12/29/2016 12:00:00 AM", 300),
      SimplePLTRecord(4, 1.0 / 5.0, 3500141, "01/10/2016 00:00", "01/09/2016 00:00", 400),
      SimplePLTRecord(4, 1.0 / 5.0, 3500141, "01/11/2016 00:00", "01/09/2016 00:00", 500),
      SimplePLTRecord(4, 1.0 / 5.0, 3500141, "1/13/2016 12:00:00 AM", "01/09/2016 00:00", 600),
      SimplePLTRecord(4, 1.0 / 5.0, 3500141, "1/14/2016 12:00:00 AM", "01/09/2016 00:00", 700),
      SimplePLTRecord(4, 1.0 / 5.0, 3500151, "8/19/2016 12:00:00 AM", "8/19/2016 12:00:00 AM", 800),
      SimplePLTRecord(5, 1.0 / 5.0, 3500166, "9/19/2016 12:00:00 AM", "9/19/2016 12:00:00 AM", 900)
    )

    spark.sparkContext.setLogLevel("WARN")
    val df = spark.createDataFrame(records).withColumn("subportfolioId", lit("all"))
    val theFixture = FixtureParam(spark, df)

    try {
      withFixture(test.toNoArgTest(theFixture))
    } finally spark.close()
  }

  "Test data set" should "have 9 records" in { fixture =>
    assert(fixture.data.count() == 9)
  }

  "Test AAL" should "have value" in { fixture =>
    val aal = PLT.calculateAAL(fixture.data).select("AAL").first().getDouble(0)
    assert(aal == 900)
  }

  "Test Standard Deviation" should "have value" in { fixture =>
    val sd = PLT.calculateStandardDeviation(fixture.data).select("StdDev").first().getDouble(0)
    // Note: This test differs from the original in the plttools repo, as
    //       the implementation of PLT.calculateStandardDeviation performs
    //       the roll up of the losses to the PeriodId, which was not done
    //       the test in the plttools repo.
    assert(Math.abs(sd - 1097.26933794) < 1e-8)
  }
}
