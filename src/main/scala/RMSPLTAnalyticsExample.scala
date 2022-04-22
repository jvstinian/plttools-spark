/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
package main.scala

import com.jvstinian.rms.aggregationtools.EPCurve
import com.jvstinian.rms.aggregationtools.PLT
import com.jvstinian.rms.aggregationtools.PLTRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

import java.sql.Date
import java.text.SimpleDateFormat
import scala.io.Source

final case class PLTRecordParseException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

object PLTRecordParser {
  def parseRecord(record: String): PLTRecord = {
    val format = new SimpleDateFormat("M/d/yyyy HH:mm:ss a")
    record.split(',') match {
      case Array(lossType, periodIdStr, weightStr, eventIdStr, eventDateStr, lossDateStr, lossStr) =>
        PLTRecord(
          lossType,
          periodIdStr.toInt,
          weightStr.toDouble,
          eventIdStr.toLong,
          new Date(format.parse(eventDateStr).getTime()),
          new Date(format.parse(lossDateStr).getTime()),
          lossStr.toDouble
        )
      case _ => throw PLTRecordParseException(f"Incorrect format for PLT record {record}")
    }
  }
}

object RMSPLTAnalyticsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("RMSPLTAnalyticsExample")
      .master("local")
      .getOrCreate()
    val pltrecs = Source
      .fromInputStream(getClass.getResourceAsStream("/plt.csv"))
      .getLines()
      .toSeq
      .tail
      .map(PLTRecordParser.parseRecord)
    val df = PLTRecord.toDataframe(spark, pltrecs).withColumn("SubportfolioId", lit("All"))
    df.show(false)

    PLT.calculateStatistics(df).show(false)

    val rps = EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble)
    val zeroLossRecordWeight = 1.0 / 50000.0
    val pmlsdf = PLT.calculateCombinedPMLForReturnPeriodsBySubportfolio(
      df,
      rps,
      Some(zeroLossRecordWeight)
    )
    pmlsdf.orderBy(col("SubportfolioId"), col("ReturnPeriod").desc).show(false)

    spark.stop()
  }
}
