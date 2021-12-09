/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
import com.jvstinian.rms.aggregationtools.EPCurve
import com.jvstinian.rms.aggregationtools.PLT
import com.jvstinian.rms.aggregationtools.SimplePLTRecord
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.scalatest.Outcome
import org.scalatest.flatspec.FixtureAnyFlatSpec


class PLTCalculator extends FixtureAnyFlatSpec {

    case class FixtureParam(spark: SparkSession, data: DataFrame) 

    def withFixture(test: OneArgTest): Outcome = {
        val spark: SparkSession = SparkSession.builder().appName("PLTCalculatorTest").master("local").getOrCreate()
        val records: Seq[SimplePLTRecord] = Seq(
            SimplePLTRecord(1, 1.0/5.0, 3500016, "3/13/2016 12:00:00 AM", "03/10/2016 00:00", 100),
            SimplePLTRecord(3, 1.0/5.0, 3500129, "8/25/2016 12:00:00 AM", "8/24/2016 12:00:00 AM", 200),
            SimplePLTRecord(3, 1.0/5.0, 3500140, "01/01/2017 00:00", "12/29/2016 12:00:00 AM", 300),
            SimplePLTRecord(4, 1.0/5.0, 3500141, "01/10/2016 00:00", "01/09/2016 00:00", 400),
            SimplePLTRecord(4, 1.0/5.0, 3500141, "01/11/2016 00:00", "01/09/2016 00:00", 500),
            SimplePLTRecord(4, 1.0/5.0, 3500141, "1/13/2016 12:00:00 AM", "01/09/2016 00:00", 600),
            SimplePLTRecord(4, 1.0/5.0, 3500141, "1/14/2016 12:00:00 AM", "01/09/2016 00:00", 700),
            SimplePLTRecord(4, 1.0/5.0, 3500151, "8/19/2016 12:00:00 AM", "8/19/2016 12:00:00 AM", 800),
            SimplePLTRecord(5, 1.0/5.0, 3500166, "9/19/2016 12:00:00 AM", "9/19/2016 12:00:00 AM", 900)
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

    "Test OEP calculations" should "all have EPType \"OEP\"" in { fixture => 
        val oepdf = PLT.calculateOEPForReturnPeriodsBySubportfolio(fixture.data, EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble), Some(1.0/5.0))
        val isOep = oepdf.withColumn("isOEPType", col("EPType") === lit("OEP")).select("isOEPType").collect.map(_.getBoolean(0))
        assert(isOep.reduce(_&&_))
    }
    
    it should "should have values" in { fixture => 
        val rps = Seq(5.0, 2.5, 5.0/3.0, 1.25, 1.0)
        val oepdf = PLT.calculateOEPForReturnPeriodsBySubportfolio(fixture.data, rps, Some(1.0/5.0))
        val res = oepdf.select("ReturnPeriod", "Loss").collect.map(row => (row.getDouble(0), row.getDouble(1)))
        val lossmap = Map(res: _*)
        assert(lossmap(5.0) == 900)
        assert(lossmap(2.5) == 800)
        assert(Math.abs(lossmap(5.0/3.0) - 300) < 1e-8)
        assert(lossmap(1.25) == 100)
        assert(lossmap(1.0) == 0.0)
    }
    
    "Test AEP calculations" should "all have EPType \"AEP\"" in { fixture => 
        val aepdf = PLT.calculateAEPForReturnPeriodsBySubportfolio(fixture.data, EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble), Some(1.0/5.0))
        val isAep = aepdf.withColumn("isAEPType", col("EPType") === lit("AEP")).select("isAEPType").collect.map(_.getBoolean(0))
        assert(isAep.reduce(_&&_))
    }

    it should "should have values" in { fixture => 
        val rps = Seq(5.0, 2.5, 5.0/3.0, 1.25, 1.0)
        val aepdf = PLT.calculateAEPForReturnPeriodsBySubportfolio(fixture.data, rps, Some(1.0/5.0))
        val res = aepdf.select("ReturnPeriod", "Loss").collect.map(row => (row.getDouble(0), row.getDouble(1)))
        val lossmap = Map(res: _*)
        assert(lossmap(5.0) == 3000)
        assert(lossmap(2.5) == 900)
        assert(Math.abs(lossmap(5.0/3.0) - 500.0) < 1e-8) // had to use approximation here
        assert(lossmap(1.25) == 100)
        assert(lossmap(1.0) == 0.0)
    }
}