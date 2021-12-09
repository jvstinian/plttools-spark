/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
package com.jvstinian.rms.aggregationtools

case class EPCurveInput(subportfolioId: String,  probability: Double, loss: Double) {
    def toEPCurveRecord() = new EPCurveRecord(probability, loss)
}

case class EPCurveResult(SubportfolioId: String, EPType: String, ReturnPeriod: Double, Loss: Double)
