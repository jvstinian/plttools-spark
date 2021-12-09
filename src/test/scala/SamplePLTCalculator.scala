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
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec

import scala.io.Source

class SamplePLTCalculator extends FixtureAnyFlatSpec {

  case class FixtureParam(spark: SparkSession, data: DataFrame)

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

    val theFixture = FixtureParam(spark, df)

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
    val oepdf = PLT.calculateOEPForReturnPeriodsBySubportfolio(fixture.data, rps, Some(zeroLossRecordWeight))
    val isOep =
      oepdf.withColumn("isOEPType", col("EPType") === lit("OEP")).select("isOEPType").collect.map(_.getBoolean(0))
    assert(isOep.reduce(_ && _))
  }

  it should "should have values" in { fixture =>
    val zeroLossRecordWeight = 1.0 / 50000.0
    val rps = Seq[Double](2, 5, 10, 25, 50, 100, 200, 250, 500, 1000, 5000, 10000)
    val oepdf = PLT.calculateOEPForReturnPeriodsBySubportfolio(fixture.data, rps, Some(zeroLossRecordWeight))
    val res = oepdf.select("ReturnPeriod", "Loss").collect.map(row => (row.getDouble(0), row.getDouble(1)))
    val lossmap = Map(res: _*)
    assert(lossmap(10000.0) == 1819634176.0)
    assert(lossmap(5000.0) == 1523913168.00)
    assert(Math.abs(lossmap(1000.0) - 845724466.4) < 1e-2)
    assert(Math.abs(lossmap(500.0) - 588621182.4) < 1e-2)
    assert(Math.abs(lossmap(250.0) - 412294994.10) < 1e-2)
    assert(Math.abs(lossmap(200.0) - 366269622.00) < 1e-2)
    assert(Math.abs(lossmap(100.0) - 242027618.20) < 1e-2)
    assert(Math.abs(lossmap(50.0) - 151068382.10) < 1e-2)
    assert(Math.abs(lossmap(25.0) - 82166268.18) < 1e-2)
    assert(Math.abs(lossmap(10.0) - 31677493.70) < 1e-2)
    assert(Math.abs(lossmap(5.0) - 11293770.08) < 1e-2)
    assert(Math.abs(lossmap(2.0) - 532981.48) < 1e-2)
  }

  /*
    "Saample OEP calculations" should "have values" in { fixture =>
        """ Test Calculate OEP Curve from Excel PLT example"""
        assert oep.loss_at_a_given_return_period(10)  ==  38559130.58
        assert oep.loss_at_a_given_return_period(25)  == 105494369.44
        assert oep.loss_at_a_given_return_period(50)  == 198673192.75
        assert oep.loss_at_a_given_return_period(100) == 339844342.61
        assert oep.loss_at_a_given_return_period(200) == 514043110.58
        assert oep.loss_at_a_given_return_period(250) == 580645121.6999999
        assert oep.loss_at_a_given_return_period(500) == 832353773.07
   */
}
