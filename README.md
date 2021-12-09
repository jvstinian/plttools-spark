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

For anyone interested in using this as a spark library 
in order to 
calculate statistics for ELTs or PLTs or to 
calculate EP curves for PLTs, 
the test cases might prove instructive.
