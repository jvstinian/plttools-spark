/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
import com.jvstinian.rms.aggregationtools.EPCurve
import com.jvstinian.rms.aggregationtools.EPCurveRecord
import com.jvstinian.rms.aggregationtools.InvalidValueException

class EPCurveSpec extends UnitSpec {
    val probs: Seq[Double] = 0.001.to(0.999).by(0.001).toSeq
    val losss: Seq[Int] = 9990.to(10).by(-10).toSeq
    val fixture: Array[EPCurveRecord] = probs.zip(losss).map{ case (p, l) => new EPCurveRecord(p, l.toDouble) }.toArray

    "Testing when no type provided to init that EP Curve Type" should "be unknown" in { 
        val f = fixture
        val curve = new EPCurve(f, null, null)
        assert(curve.getEPType() == EPCurve.EPType.UNKNOWN)
    }
    
    "Testing EPCurve" should "have a non-empyt array of standard return periods" in {
        assert(EPCurve.RETURN_PERIODS.length > 0)
    }

    "Testing loss curve values" should "equal provided values" in {
        val oepCurve = new EPCurve(fixture, EPCurve.EPType.OEP, 0.001)
        val loss_10_year = oepCurve.getLossAtReturnPeriod(10)
        val loss_100_year = oepCurve.getLossAtReturnPeriod(100)
        val loss_1000_year = oepCurve.getLossAtReturnPeriod(1000)
        val loss_1000000_year = oepCurve.getLossAtReturnPeriod(1000000)
        val loss_800_year = oepCurve.getLossAtReturnPeriod(800)
        assert(oepCurve.getEPType() == EPCurve.EPType.OEP)
        assert(loss_10_year == 9000.0)
        assert(loss_100_year == 9900.0)
        assert(loss_1000_year == 9990.0)
        assert(loss_1000000_year == 9990.0)
        assert(loss_800_year == 9987.5)
    }

    "Testing loss at negative return period" should "throw exception" in {
        val oepCurve = new EPCurve(fixture, EPCurve.EPType.OEP, 0.001)
        assertThrows[InvalidValueException] {
            oepCurve.getLossAtReturnPeriod(-1.0)
        }
    } 
}
