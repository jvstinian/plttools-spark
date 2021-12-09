/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
package main.scala

import com.jvstinian.rms.aggregationtools.PLTRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.sql.Date
import java.text.SimpleDateFormat
import scala.io.Source

final case class PLTRecordParseException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

object PLTRecordParser {
  type PLTRecordType = (String, Int, Double, Long, Date, Date, Double)
  val PLTSchema: StructType = StructType(
    StructField("LossType", StringType, false) ::
      StructField("PeriodId", IntegerType, false) ::
      StructField("Weight", DoubleType, false) ::
      StructField("EventId", LongType, false) ::
      StructField("EventDate", DateType, false) ::
      StructField("LossDate", DateType, false) ::
      StructField("Loss", DoubleType, false) :: Nil
  )

  def parseRecord(record: String): PLTRecord = {
    val format = new SimpleDateFormat("M/d/yyyy HH:mm:ss a")
    record.split(',') match {
      case Array(lossType, periodIdStr, weightStr, eventIdStr, eventDateStr, lossDateStr, lossStr) =>
        PLTRecord(
          lossType,
          periodIdStr.toInt,
          weightStr.toDouble, /*BigInt(eventIdStr)*/ eventIdStr.toLong,
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
    // Source.fromInputStream(getClass.getResourceAsStream("/plt.csv")).getLines().foreach(println)
    val pltrecs = Source
      .fromInputStream(getClass.getResourceAsStream("/plt.csv"))
      .getLines()
      .toSeq
      .tail
      .map(PLTRecordParser.parseRecord)
    val df = PLTRecord.toDataframe(spark, pltrecs)
    df.show(false)

    spark.stop()
  }
}
