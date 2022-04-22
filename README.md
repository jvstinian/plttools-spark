# PLTTools for Spark

A spark package for processing
RMS ELT (Event Loss Table) and PLT (Period Loss Table)
data.

This is largely based on the work found in the 
[plttools](https://github.com/RMS-Consulting/plttools)
repo (MIT License).  
Since we use spark for data processing, the 
code structure is somewhat different than what 
is found in the `plttools` repo.  

# Building and Running 

To build and run, you will need to install 

* Java 8, 
* Scala 2.11.12, 
* Spark 2.4.5, and 
* sbt 1.3.13, 

or alternatively a cloud service (e.g., EMR) with this software.  

A Dockerfile has been provided for demonstration purposes.  
To build the docker image, run 
```
make build-docker
# this builds a docker image with name and tag plttools-spark:latest
```
To run the simple example that's been set up in the main class, use 
```
make run-docker
```
Test cases can be run with 
```
make test-docker
```

# Using as a Spark Package for PLT Statistics

We describe how to use this package to calculate 
statistics for PLT records.

We assume that a Spark DataFrame `df` has been defined 
with the following fields: 

| Field Name     | Type      | Description                                                                       |
| -------------- | --------- | --------------------------------------------------------------------------------- |
| SubportfolioId | String    | A subportfolio identifier used for grouping in the calculations of the statistics |
| PeriodId       | Integer   | A period identifier                                                               |
| EventId        | Integer   | The event identifier                                                              |
| EventDate      | Timestamp | The date of the simulated event                                                   |
| Weight         | Double    | The weight associated with the period                                             |
| Loss           | Double    | The loss associated with the period and event                                     |

To calculate Average Annual Loss (AAL) and the Standard Deviation, use the following
```
spark> val stats = PLT.calculateStatistics(df)
spark> stats.show(false)
```

`stats` is a DataFrame containing the PLT statistics AAL, 
Standard Deviation, and Coefficient of Variation by subportfolio ID.  

To calculate the tail statistics, one can use something like 
```
spark> val rps = EPCurve.RETURN_PERIODS.toSeq.map(_.toDouble) // Or: Array[Double](...) for custom values
spark> val numberOfPeriods = 50000.0 // Specify the total number of periods considered
spark> val zeroLossRecordWeight =  1.0 / numberOfPeriods
spark> val pmlsdf = PLT.calculateCombinedPMLForReturnPeriodsBySubportfolio(df, rps, Some(zeroLossRecordWeight))
spark> // print
spark> pmlsdf.orderBy(col("SubportfolioId"), col("ReturnPeriod").desc).show(false)
```

The resulting DataFrame `pmlsdf` contains the _Occurrence Probable Maximum Loss_ (OPML) and
_Aggregate Probable Maximum Loss_ (APML) by subportfolio ID and return period.  
We use the terminology _Probable Maximum Loss_ as described in [1] here as 
we are looking at loss levels associated with return periods.  
Note that in the documentation from RMS and 
in the code repository referenced above, RMS often extends the use of the term 
_Exceedance Probability_ to describe these analytics as well 
(i.e., you might see these analytics referred to as OEP and AEP in RMS documents).

For anyone interested in using other methods found in this package 
for calculating statistics for ELTs or PLTs,
or for calculating EP curves for PLTs,
the test cases might prove instructive.

# References

1. David Homer and Ming Li, [Notes on Using Property Catastrophe Model Results](https://www.casact.org/sites/default/files/2021-02/2017_most-practical-paper_homer-li.pdf), _Casualty Actuarial Society E-Forum_, Spring 2017-Volume 2 