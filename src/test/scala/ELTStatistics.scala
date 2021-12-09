/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
import com.jvstinian.rms.aggregationtools.ELT
import com.jvstinian.rms.aggregationtools.ELTRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec

class ELTStatistics extends FixtureAnyFlatSpec {

  case class FixtureParam(spark: SparkSession, data: DataFrame)

  def withFixture(test: OneArgTest): Outcome = {
    val spark: SparkSession = SparkSession.builder().appName("ELTStatisticsTest").master("local").getOrCreate()
    val records: Seq[ELTRecord] = Seq(
      ELTRecord("All", 100, 0.000018, 68094, 26950.83592, 85533, 300000),
      ELTRecord("All", 200, 0.00021, 50713, 16961.56092, 18734, 300000),
      ELTRecord("All", 300, 0.000305, 3281, 6019, 12004, 50000),
      ELTRecord("All", 350, 0.000411, 27540, 48962, 17761, 210000),
      ELTRecord("All", 400, 0.000912, 98367, 16085.91583, 53815, 120000),
      ELTRecord("All", 500, 0.000078, 19025, 75853, 58268, 500000),
      ELTRecord("All", 526, 0.000729, 44536, 7441, 22090, 300000),
      ELTRecord("All", 600, 0.000042, 84287, 62429, 181382, 200000)
    )

    spark.sparkContext.setLogLevel("WARN")
    val df = spark.createDataFrame(records)
    val theFixture = FixtureParam(spark, df)

    try {
      withFixture(test.toNoArgTest(theFixture))
    } finally spark.close()
  }

  "Test data set" should "have 8 records" in { fixture =>
    assert(fixture.data.count() == 8)
  }

  "Test AAL" should "have value" in { fixture =>
    val aal = ELT.calculateStatistics(fixture.data).select("AAL").first().getDouble(0)
    assert(Math.abs(aal - 151.3) < 0.1)
  }

  "Test Standard Deviation" should "have value" in { fixture =>
    val sd = ELT.calculateStatistics(fixture.data).select("StdDev").first().getDouble(0)
    assert(Math.abs(sd - 4790.8) < 0.1)
  }

  "Test Coefficient of Variation " should "have value" in { fixture =>
    val cv = ELT.calculateStatistics(fixture.data).select("CV").first().getDouble(0)
    assert(Math.abs(cv - 31.64) < 0.1)
  }
}
