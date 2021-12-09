/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
import com.jvstinian.rms.aggregationtools.ELT
import com.jvstinian.rms.aggregationtools.ELTRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec

class ELTRollupSpec extends FixtureAnyFlatSpec {

  case class FixtureParam(spark: SparkSession, data: DataFrame)

  def withFixture(test: OneArgTest): Outcome = {
    val spark: SparkSession = SparkSession.builder().appName("ELTStatisticsTest").master("local").getOrCreate()

    val eventIds: Seq[Long] = Seq(100, 200, 300, 400, 500, 600, 100, 200, 350, 400, 526)
    val rates: Seq[Double] =
      Seq(0.000018, 0.00021, 0.000305, 0.000912, 0.000078, 0.000042, 0.000018, 0.00021, 0.000411, 0.000912, 0.000729)
    val losses: Seq[Double] = Seq(51765, 30931, 3281, 21980, 19025, 84287, 16329, 19782, 27540, 76387, 44536)
    val sdis: Seq[Double] = Seq(2591, 10482, 6019, 15272, 75853, 62429, 26826, 13335, 48962, 5052, 7441)
    val sdcs: Seq[Double] = Seq(67373, 13942, 12004, 49023, 58268, 181382, 18160, 4792, 17761, 4792, 22090)
    val expvals: Seq[Double] = Seq(200000, 200000, 50000, 50000, 500000, 200000, 100000, 100000, 210000, 70000, 300000)

    val records: Seq[ELTRecord] = eventIds.zip(rates).zip(losses).zip(sdis).zip(sdcs).zip(expvals).map {
      case (((((eventId, rate), loss), sdi), sdc), expval) =>
        ELTRecord("All", eventId, rate, loss, sdi, sdc, expval)
    }

    spark.sparkContext.setLogLevel("WARN")
    val df = spark.createDataFrame(records)
    val theFixture = FixtureParam(spark, df)

    try {
      withFixture(test.toNoArgTest(theFixture))
    } finally spark.close()
  }

  "Test data set" should "have 11 records" in { fixture =>
    assert(fixture.data.count() == 11)
  }

  "Test rollup of record set" should "have 8 records" in { fixture =>
    val df = ELT.rollUp(fixture.data)
    assert(df.count() == 8)
  }

  it should "have loss of 68094 for event ID 100" in { fixture =>
    val loss = ELT.rollUp(fixture.data).filter(col("EventId") === 100).select("Loss").first().getDouble(0)
    assert(loss == 68094)
  }
}
