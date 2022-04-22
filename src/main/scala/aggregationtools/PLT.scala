/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
 */
package com.jvstinian.rms.aggregationtools

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.pow
import org.apache.spark.sql.functions.sqrt
import org.apache.spark.sql.functions.sum

import java.sql.Date

case class SimplePLTRecord(
    periodId: Int,
    weight: Double,
    eventId: Long,
    eventDate: String,
    lossDate: String,
    loss: Double
)

// PLTRecord is used for construction a Spark DataFrame.
// Though this runs contrary to naming conventions,
// we use uppercase constructor parameters for consistency
// with the input field names (and to avoid manual renaming
// of fields).
case class PLTRecord(
    LossType: String,
    PeriodId: Int,
    Weight: Double,
    EventId: Long,
    EventDate: Date,
    LossDate: Date,
    Loss: Double
)

object PLTRecord {
  def toDataframe(spark: SparkSession, pltrecs: Seq[PLTRecord]): DataFrame = {
    spark.createDataFrame(pltrecs)
  }
}

// This object assumes that weight is a function of periodId
object PLT {
  val calculateEPForReturnPeriods
      : ((EPCurve.EPType, Seq[Double], Option[Double]) => (String, Iterator[EPCurveInput]) => Seq[EPCurveResult]) = {
    case (epType, rps, zeroLossRecordWeightO) => {
      case (subportfolioId, rows) => {
        val epCurveRecords = rows.toArray.map(_.toEPCurveRecord())
        val epCurve = new EPCurve(epCurveRecords, epType, zeroLossRecordWeightO.map(Double.box).orNull)
        val pmls = epCurve.getLossesAtReturnPeriods(rps.toArray)
        rps.zip(pmls).map { case (rp, pml) => EPCurveResult(subportfolioId, epType.toString(), rp, pml) }
      }
    }
  }

  def calculateOEPForReturnPeriodsBySubportfolio(
      pltdf: DataFrame,
      rps: Seq[Double],
      zeroLossRecordWeight: Option[Double]
  ): DataFrame = {
    val maxPeriodLosses = pltdf
      .groupBy(col("SubportfolioId"), col("PeriodId"))
      .agg(max("Weight").alias("Weight"), max("Loss").alias("Loss"))
      .withColumn(
        "Probability",
        sum(col("Weight")).over(
          Window
            .partitionBy("SubportfolioId")
            .orderBy(col("Loss").desc)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )

    import maxPeriodLosses.sparkSession.implicits._
    maxPeriodLosses
      .map(row =>
        EPCurveInput(row.getAs[String]("SubportfolioId"), row.getAs[Double]("Probability"), row.getAs[Double]("Loss"))
      )
      .groupByKey(_.subportfolioId)
      .flatMapGroups(calculateEPForReturnPeriods(EPCurve.EPType.OEP, rps, zeroLossRecordWeight))
      .toDF()
  }

  def calculateAEPForReturnPeriodsBySubportfolio(
      pltdf: DataFrame,
      rps: Seq[Double],
      zeroLossRecordWeight: Option[Double]
  ): DataFrame = {
    val aggPeriodLosses = pltdf
      .groupBy(col("SubportfolioId"), col("PeriodId"))
      .agg(max("Weight").alias("Weight"), sum("Loss").alias("Loss"))
      .withColumn(
        "Probability",
        sum(col("Weight")).over(
          Window
            .partitionBy("SubportfolioId")
            .orderBy(col("Loss").desc)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )

    import aggPeriodLosses.sparkSession.implicits._
    aggPeriodLosses
      .map(row =>
        EPCurveInput(row.getAs[String]("SubportfolioId"), row.getAs[Double]("Probability"), row.getAs[Double]("Loss"))
      )
      .groupByKey(_.subportfolioId)
      .flatMapGroups(calculateEPForReturnPeriods(EPCurve.EPType.AEP, rps, zeroLossRecordWeight))
      .toDF()
  }

  def calculateCombinedPMLForReturnPeriodsBySubportfolio(
      pltdf: DataFrame,
      rps: Seq[Double],
      zeroLossRecordWeight: Option[Double]
  ): DataFrame = {
    val groupedPltDf = this.groupPlts(pltdf).cache
    val opmldf = this.calculateOEPForReturnPeriodsBySubportfolio(groupedPltDf, rps, zeroLossRecordWeight)
    val apmldf = this.calculateAEPForReturnPeriodsBySubportfolio(groupedPltDf, rps, zeroLossRecordWeight)
    val resdf = opmldf.union(apmldf)
    resdf
      .groupBy("SubportfolioId", "ReturnPeriod")
      .pivot("EPType", Seq[String]("OEP", "AEP"))
      .agg(max(col("Loss")).alias("Loss"))
      .withColumnRenamed("OEP", "OPML")
      .withColumnRenamed("AEP", "APML")
  }

  def groupPlts(pltdf: DataFrame): DataFrame = pltdf
    .select("SubportfolioId", "PeriodId", "EventId", "EventDate", "Weight", "Loss")
    .groupBy("SubportfolioId", "PeriodId", "EventId", "EventDate")
    .agg(max(col("Weight")).alias("Weight"), sum(col("Loss")).alias("Loss"))

  def rollUp(pltdf: DataFrame): DataFrame = pltdf
    .select("SubportfolioId", "PeriodId", "Weight", "Loss")
    .groupBy("SubportfolioId", "PeriodId")
    .agg(max(col("Weight")).alias("Weight"), sum(col("Loss")).alias("Loss"))

  def calculateAAL(pltdf: DataFrame): DataFrame = this
    .rollUp(pltdf)
    .groupBy("SubportfolioId")
    .agg(sum(col("Weight") * col("Loss")).alias("AAL"))

  def calculateStandardDeviation(pltdf: DataFrame): DataFrame = this
    .rollUp(pltdf)
    .groupBy("SubportfolioId")
    .agg(
      sum(col("Weight") * pow(col("Loss"), 2.0)).alias("MeanSquareLoss"),
      sum(col("Weight") * col("Loss")).alias("MeanLoss")
    )
    .withColumn("Variance", col("MeanSquareLoss") - pow(col("MeanLoss"), 2.0))
    .withColumn("StdDev", sqrt(col("Variance")))
    .drop("MeanSquareLoss", "MeanLoss", "Variance")

  def calculateStatistics(pltdf: DataFrame): DataFrame = this
    .rollUp(pltdf)
    .groupBy("SubportfolioId")
    .agg(
      sum(col("Weight") * pow(col("Loss"), 2.0)).alias("MeanSquareLoss"),
      sum(col("Weight") * col("Loss")).alias("AAL")
    )
    .withColumn("Variance", col("MeanSquareLoss") - pow(col("AAL"), 2.0))
    .withColumn("StdDev", sqrt(col("Variance")))
    .withColumn("CV", col("StdDev") / col("AAL"))
    .drop("MeanSquareLoss", "Variance")

}
