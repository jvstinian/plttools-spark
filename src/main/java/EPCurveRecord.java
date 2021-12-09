/*
 * MIT License
 * Copyright 2021 Justin Smith, jvstinian.com
*/
package com.jvstinian.rms.aggregationtools;

public class EPCurveRecord implements Comparable<EPCurveRecord> {
    private double probability;
    private double loss;

    public EPCurveRecord(double p, double l) {
        this.probability = p;
        this.loss = l;
    }

    public int compareTo(EPCurveRecord o) {
        return Double.compare(this.probability, o.probability);
    }

    public double getProbability() { 
        return this.probability;
    }
    
    public double getLoss() { 
        return this.loss;
    }
}
