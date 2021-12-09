/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
package com.jvstinian.rms.aggregationtools

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.pow
import org.apache.spark.sql.functions.sqrt
import org.apache.spark.sql.functions.sum

case class ELTRecord(SubportfolioId: String, EventId: Long, Rate: Double, Loss: Double, StdDevI: Double, StdDevC: Double, ExpValue: Double)

object ELT {
    def rollUp(eltdf: DataFrame): DataFrame = {
        val df = eltdf.select("SubportfolioId", "EventId", "Rate", "Loss", "StdDevI", "StdDevC", "ExpValue")
        // Note: In the original python code, df is split up into two dataframes based 
        //       on whether EventId is unique or not.
        val aggdf = df.groupBy("SubportfolioId", "EventId")
                      .agg( max(col("Rate")).alias("Rate"), 
                            sum(col("Loss")).alias("Loss"), 
                            sum(col("StdDevC")).alias("StdDevC"), 
                            sum(col("ExpValue")).alias("ExpValue"),
                            sum(pow(col("StdDevI"), 2.0)).alias("VarI") )
                      .withColumn("StdDevI", sqrt(col("VarI")))
                      .drop("VarI")
        aggdf
    }
    
    def calculateStatistics(eltdf: DataFrame): DataFrame = this.rollUp(eltdf)
            .withColumn("StdDevT", col("StdDevI") + col("StdDevC"))
            .withColumn("CV", col("StdDevT") / col("Loss"))
            .withColumn("VarianceTerm", col("Rate") * pow(col("Loss"), 2.0) * (lit(1.0) + pow(col("CV"), 2.0)))
            .groupBy("SubportfolioId")
            .agg( sum(col("Rate")*col("Loss")).alias("AAL"), 
                  sum(col("VarianceTerm")).alias("Variance") )
            .withColumn("StdDev", sqrt(col("Variance")))
            .withColumn("CV", col("StdDev")/col("AAL"))
            .drop("Variance")
}
