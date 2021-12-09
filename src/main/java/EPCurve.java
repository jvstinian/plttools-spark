/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
package com.jvstinian.rms.aggregationtools;

import java.util.Arrays;
import com.jvstinian.rms.aggregationtools.EPCurveRecord;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.commons.math3.analysis.UnivariateFunction;

public class EPCurve {
    public enum EPType {
        OEP,
        AEP,
        UNKNOWN
    };

    public static int[] RETURN_PERIODS = new int[] {2, 5, 10, 25, 50, 100, 200, 250, 500, 1000, 5000, 10000};

    private EPType epType;
    private double[] probabilities;
    private double[] losses;

    public EPCurve(EPCurveRecord[] curveRecords, EPType _epType, Double zeroLossRecordWeight) {
        Double zeroLossProbability = null;
        if(zeroLossRecordWeight != null) {
            double maxprob = 0.0;
            boolean hasZeroLoss = false;
            for (int i = 0; i < curveRecords.length; i++) {
                if (curveRecords[i].getLoss() > 0.0) {
                    maxprob = Math.max(maxprob, curveRecords[i].getProbability());
                } else if (curveRecords[i].getLoss() == 0.0) {
                    hasZeroLoss = true;
                }
            }
            if (!hasZeroLoss && (maxprob < 1.0)) {
                zeroLossProbability = Math.min(1.0, maxprob + zeroLossRecordWeight.doubleValue());
            }
        }

        this.epType = _epType != null ? _epType : EPType.UNKNOWN;
        int N = curveRecords.length + (zeroLossProbability != null ? 1 : 0);
        EPCurveRecord[] curveRecordsExt = new EPCurveRecord[N];
        System.arraycopy(curveRecords, 0, curveRecordsExt, 0, curveRecords.length);
        if(zeroLossProbability != null) {
            curveRecordsExt[N-1] = new EPCurveRecord(zeroLossProbability.doubleValue(), 0.0);
        }
        Arrays.sort(curveRecordsExt);

        this.probabilities = new double[N];
        this.losses = new double[N];
        for (int i = 0; i < N; i++) {
            this.probabilities[i] = curveRecordsExt[i].getProbability();
            this.losses[i] = curveRecordsExt[i].getLoss();
        }
    }

    public double getLossAtProbability(double prob) {
        int n = this.probabilities.length;
        if (n==0) return 0.0;

        double maxp = this.probabilities[n-1];
        double minp = this.probabilities[0];
        if (prob > maxp) {
            return 0.0;
        } else if (prob <= minp) {
            return this.losses[0];
        } else {
            // Note that to arrive here, we must have n > 1
            PolynomialSplineFunction psf = (new LinearInterpolator()).interpolate(this.probabilities, this.losses);
            return psf.value(prob);
        }
    }

    public double getLossAtReturnPeriod(double returnPeriod) throws InvalidValueException {
        if (returnPeriod <= 0.0) {
            throw new InvalidValueException(String.format("return period must be positive, got %f", returnPeriod));
        }

        double probability = 1.0 / returnPeriod;
        return this.getLossAtProbability(probability);
    }

    public EPType getEPType() {
        return this.epType;
    }
    
    public double[] getLossesAtReturnPeriods(double[] returnPeriods) throws InvalidValueException {
        double[] ret = new double[returnPeriods.length];
        for (int i = 0; i < returnPeriods.length; i++) {
            ret[i] = this.getLossAtReturnPeriod(returnPeriods[i]);
        }
        return ret;
    }
}