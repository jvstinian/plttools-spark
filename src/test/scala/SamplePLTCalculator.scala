/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
import com.jvstinian.rms.aggregationtools.EPCurve
import com.jvstinian.rms.aggregationtools.PLT
import com.jvstinian.rms.aggregationtools.PLTRecord
import main.scala.PLTRecordParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.min
import org.scalactic.Tolerance
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec

import scala.io.Source

import Tolerance.convertNumericToPlusOrMinusWrapper

class SamplePLTCalculatorSpec extends FixtureAnyFlatSpec {

  case class FixtureParam(spark: SparkSession, data: DataFrame, groupedplts: DataFrame)

  def withFixture(test: OneArgTest): Outcome = {
    val spark: SparkSession = SparkSession.builder().appName("PLTCalculatorTest").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val pltrecs = Source
      .fromInputStream(getClass.getResourceAsStream("/plt.csv"))
      .getLines()
      .toSeq
      .tail
      .map(PLTRecordParser.parseRecord)
    val df = PLTRecord.toDataframe(spark, pltrecs).withColumn("subportfolioId", lit("all"))
    val groupeddf = PLT.groupPlts(df).cache

    val theFixture = FixtureParam(spark, df, groupeddf)

    try {
      withFixture(test.toNoArgTest(theFixture))
    } finally spark.close()
  }

  "Sample data set" should "have 155725 records" in { fixture =>
    assert(fixture.data.count() == 155725)
  }

  "Weights" should "be positive" in { fixture =>
    val minWeight = fixture.data.agg(min(col("weight"))).first().getDouble(0)
    assert(minWeight == 0.00002)
  }

  "Test OEP calculations" should "all have EPType \"OEP\"" in { fixture =>
    val zeroLossRecordWeight = 1.0 / 50000.0
    val rps = EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble)
    val oepdf = PLT.calculateOEPForReturnPeriodsBySubportfolio(fixture.groupedplts, rps, Some(zeroLossRecordWeight))
    val isOep =
      oepdf.withColumn("isOEPType", col("EPType") === lit("OEP")).select("isOEPType").collect.map(_.getBoolean(0))
    assert(isOep.reduce(_ && _))
  }

  it should "should have values" in { fixture =>
    val zeroLossRecordWeight = 1.0 / 50000.0
    val rps = EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble)
    val oepdf = PLT.calculateOEPForReturnPeriodsBySubportfolio(fixture.groupedplts, rps, Some(zeroLossRecordWeight))
    val res = oepdf.select("ReturnPeriod", "Loss").collect.map(row => (row.getDouble(0), row.getDouble(1)))
    val lossmap = Map(res: _*)
    assert(lossmap(50000.0) === (3961210646.25 +- 1e-2))
    assert(lossmap(10000.0) === (2566817596.18 +- 1e-2))
    assert(lossmap(5000.0) === (1982672189.0 +- 1e-2))
    assert(lossmap(1000.0) === (1149500694.43 +- 1e-2))
    assert(lossmap(500.0) === (832353773.07 +- 1e-2))
    assert(lossmap(250.0) === (580645121.7 +- 1e-2))
    assert(lossmap(200.0) === (514043110.58 +- 1e-2))
    assert(lossmap(100.0) === (339844342.61 +- 1e-2))
    assert(lossmap(50.0) === (198673192.75 +- 1e-2))
    assert(lossmap(25.0) === (105494369.44 +- 1e-2))
    assert(lossmap(10.0) === (38559130.58 +- 1e-2))
    assert(lossmap(5.0) === (13233579.61 +- 1e-2))
    assert(lossmap(2.0) === (583871.12 +- 1e-2))
  }

  "Test AEP calculations" should "all have EPType \"AEP\"" in { fixture =>
    val zeroLossRecordWeight = 1.0 / 50000.0
    val rps = EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble)
    val aepdf = PLT.calculateAEPForReturnPeriodsBySubportfolio(fixture.groupedplts, rps, Some(zeroLossRecordWeight))
    val isAep =
      aepdf.withColumn("isAEPType", col("EPType") === lit("AEP")).select("isAEPType").collect.map(_.getBoolean(0))
    assert(isAep.reduce(_ && _))
  }

  // In the original python test cases, there was no test
  // for the AEP calculations for the sample csv file.
  // We have used the Jupyter notebook to generate values
  // for the standard return periods, and have added the
  // following test for the AEP calculations using spark.
  it should "should have values" in { fixture =>
    val zeroLossRecordWeight = 1.0 / 50000.0
    val rps = EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble)
    val aepdf = PLT.calculateAEPForReturnPeriodsBySubportfolio(fixture.groupedplts, rps, Some(zeroLossRecordWeight))
    val res = aepdf.select("ReturnPeriod", "Loss").collect.map(row => (row.getDouble(0), row.getDouble(1)))
    val lossmap = Map(res: _*)
    assert(lossmap(50000.0) === (4458481476.37 +- 1e-2))
    assert(lossmap(10000.0) === (2567302366.31 +- 1e-2))
    assert(lossmap(5000.0) === (2066233259.4 +- 1e-2))
    assert(lossmap(1000.0) === (1268780632.3200002 +- 1e-2))
    assert(lossmap(500.0) === (901186582.9999999 +- 1e-2))
    assert(lossmap(250.0) === (631468361.0500001 +- 1e-2))
    assert(lossmap(200.0) === (557699753.47 +- 1e-2))
    assert(lossmap(100.0) === (369668338.56 +- 1e-2))
    assert(lossmap(50.0) === (225176137.1 +- 1e-2))
    assert(lossmap(25.0) === (120435807.74 +- 1e-2))
    assert(lossmap(10.0) === (44769969.04 +- 1e-2))
    assert(lossmap(5.0) === (15443202.180000002 +- 1e-2))
    assert(lossmap(2.0) === (672148.1900000001 +- 1e-2))
  }

  "Test AAL" should "have value" in { fixture =>
    val aal = PLT.calculateAAL(fixture.data).select("AAL").first().getDouble(0)
    assert(aal === (21798489.1307784 +- 1e-6))
  }

  "Test Standard Deviation" should "have value" in { fixture =>
    val sd = PLT.calculateStandardDeviation(fixture.data).select("StdDev").first().getDouble(0)
    assert(sd === (91165041.19825359 +- 1e-6))
  }

  "Test statistics" should "have columns" in { fixture =>
    val stats = PLT.calculateStatistics(fixture.data)
    assert(stats.columns.deep == Array[String]("SubportfolioId", "AAL", "StdDev", "CV").deep)
  }

  it should "have values" in { fixture =>
    val stats = PLT.calculateStatistics(fixture.data).cache
    val aal = stats.first().getDouble(1)
    val sd = stats.first().getDouble(2)
    val cv = stats.first().getDouble(3)
    assert(aal === (21798489.1307784 +- 1e-6))
    assert(sd === (91165041.19825359 +- 1e-6))
    assert(cv === (4.182172473115717 +- 1e-6))
  }
}
