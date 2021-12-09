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

case class PLTRecord(
    lossType: String,
    periodId: Int,
    weight: Double,
    eventId: Long,
    eventDate: Date,
    lossDate: Date,
    loss: Double
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
      .groupBy(col("subportfolioId"), col("periodId"))
      .agg(max("weight").alias("weight"), max("loss").alias("loss"))
      .withColumn(
        "probability",
        sum(col("weight")).over(
          Window
            .partitionBy("subportfolioId")
            .orderBy(col("loss").desc)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )

    import maxPeriodLosses.sparkSession.implicits._
    maxPeriodLosses
      .map(row =>
        EPCurveInput(row.getAs[String]("subportfolioId"), row.getAs[Double]("probability"), row.getAs[Double]("loss"))
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
      .groupBy(col("subportfolioId"), col("periodId"))
      .agg(max("weight").alias("weight"), sum("loss").alias("loss"))
      .withColumn(
        "probability",
        sum(col("weight")).over(
          Window
            .partitionBy("subportfolioId")
            .orderBy(col("loss").desc)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )

    import aggPeriodLosses.sparkSession.implicits._
    aggPeriodLosses
      .map(row =>
        EPCurveInput(row.getAs[String]("subportfolioId"), row.getAs[Double]("probability"), row.getAs[Double]("loss"))
      )
      .groupByKey(_.subportfolioId)
      .flatMapGroups(calculateEPForReturnPeriods(EPCurve.EPType.AEP, rps, zeroLossRecordWeight))
      .toDF()
  }

  def rollUp(pltdf: DataFrame): DataFrame = pltdf
    .select("subportfolioId", "periodId", "weight", "loss")
    .groupBy("subportfolioId", "periodId")
    .agg(max(col("weight")).alias("weight"), sum(col("loss")).alias("loss"))

  def calculateAAL(pltdf: DataFrame): DataFrame = this
    .rollUp(pltdf)
    .groupBy("subportfolioId")
    .agg(sum(col("weight") * col("loss")).alias("AAL"))

  def calculateStandardDeviation(pltdf: DataFrame): DataFrame = this
    .rollUp(pltdf)
    .groupBy("subportfolioId")
    .agg(
      sum(col("weight") * pow(col("loss"), 2.0)).alias("MeanSquareLoss"),
      sum(col("weight") * col("loss")).alias("MeanLoss")
    )
    .withColumn("Variance", col("MeanSquareLoss") - pow(col("MeanLoss"), 2.0))
    .withColumn("StdDev", sqrt(col("Variance")))
    .drop("MeanSquareLoss", "MeanLoss", "Variance")

  def calculateStatistics(pltdf: DataFrame): DataFrame = this
    .rollUp(pltdf)
    .groupBy("subportfolioId")
    .agg(
      sum(col("weight") * pow(col("loss"), 2.0)).alias("MeanSquareLoss"),
      sum(col("weight") * col("loss")).alias("AAL")
    )
    .withColumn("Variance", col("MeanSquareLoss") - pow(col("AAL"), 2.0))
    .withColumn("StdDev", sqrt(col("Variance")))
    .withColumn("CV", col("StdDev") / col("AAL"))
    .drop("MeanSquareLoss", "Variance")

}
