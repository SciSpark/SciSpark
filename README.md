SciSpark
====

![picture alt](http://image.slidesharecdn.com/jljkdhlxtlgwcyboil6n-signature-c9af2d5a7f730d5a4779821a7bd1f0333657fd7c0430ac7965a5576c08924b8a-poli-150624001008-lva1-app6891/95/spark-at-nasajplchris-mattmann-nasajpl-29-638.jpg?cb=1435104721)
#Update
SciSpark can now read Netcdf files from HDFS using SciSparkContext.NetcdfDFSFile by leveraging the binaryFiles function.
The function binaryFiles opens the entire byte array and we use NetcdfFile.openInMemory() to convert the byte-array into a NetcdfDataset object. 

We want to read Netcdf variables given their offsets, without loading the entire file into memory. 
If any visitors have insights into achieving this, please see the issue titled 
"Reading variables without loading entire Netcdf File".

#Introduction

[SciSpark](http://esto.nasa.gov/forum/estf2015/presentations/Mattmann_S1P8_ESTF2015.pdf) is a scalable scientific processing platform that makes interactive computation and exploration possible. This prototype project outlines the architecture of the scientific RDD (sRDD), and a library of linear algebra and geospatial operations. Its initial focus is expressing [Grab em' Tag em' Graph em' algorithm](https://github.com/kwhitehall/grab-tag-graph) as a map-reduce algorithm. 

#Installation

Requirements (in-order):
* ND4J: https://github.com/deeplearning4j/nd4j
* ND4S: https://github.com/deeplearning4j/nd4s
* Scala: v2.10.5+
* Maven 2.0x+
* SBT: v0.13.5+

Steps:

1. Ensure ND4J and ND4S are installed in you local Maven repository
2. Open a terminal, and enter: `sbt clean assembly`
3. You should see a [success] message at the bottom if the package built correctly.

Test your installation:

```sbt "run"```

Possible pitfalls:

ND4S + Scala compilation issues: If problems occur, consider changing the scala version in the build.sbt file of the nd4s to scala version 2.10. Update the libraries dependenices in build.sbt in scispark accordingly. 
* Note that the current SciSpark build includes an nd4s jar that is part of the classpath. 
The jar is created by downloading nd4s from github and cbanging the scala version to 2.10. The jar is then copied
under /lib in the SciSparkTestExperiments directory. 

#Getting Started

We extend the following functionality for users trying to use scientific datasets
like [NetCDF](http://www.unidata.ucar.edu/software/netcdf/) on Apache Spark.
A file containing a list of paths to NetCDF files must be created.
We have provided a file containing 6000+ NetCDF URL's to use.

The following example connects to a spark instance running on localhost:7077.
It reads the Netcdf files and loads the "data" variable array into tensor objects.
The array is masked for values under 241.0, and then reshaped by averaging blocks of dimension 20 x 20.

Finally the arrays are summed together in a reduce operation.


```
val sc = new SciSparkContext("spark://localhost:7077", "SciSpark Program") 
```
```
val scientificRDD = sc.NetcdfFile("TRMM_L3_Links.txt", List("data")) 
```
```
val filteredRDD = scientificRDD.map(p => p("data") <= 241.0) 
```
```
val reshapedRDD = filteredRDD.map(p => p.reduceResolution(20) 
```
```
val sumAllRDD = reshapedRDD.reduce(_ + _) 
```
```
println(sumAllRDD)
```

#Grab em' Tag em' Graph em' : An Automated Search for Mesoscale Convective Complexes

[GTG](http://static1.squarespace.com/static/538b31b5e4b02d5bb7053eba/t/53ea7f48e4b00015c3fcc5d3/1407876936029/KDW_ThesisFinal.pdf) is an algorithm designed to search for Mesoscale Convective Complexes given temperature brightness data.
 
 The map-reduce implementation of GTG is found in the following files : 
 
 [MainMergHDFS](https://github.com/rahulpalamuttam/SciSparkTestExperiments/blob/master/src/main/scala/org/dia/algorithms/mcc/MainMergHDFS.scala) : Reads all MERG files from a directory in HDFS
 
 [MainMergRandom](https://github.com/rahulpalamuttam/SciSparkTestExperiments/blob/master/src/main/scala/org/dia/algorithms/mcc/MainMergRandom.scala)  : Generates random arrays without any source. The randomness is seeded by the hash value of your input paths, so the same input will yield the same output.
##Project Status

This project is in active development.
Currently it is being refactored as the first phase of development has concluded
with a focus on prototyping and designing GTG as a map-reduce algorithm.

##Want to Use or Contribute to this Project?
Contact us at ....

##Technology
This project utilizes the following open-source technologies [Apache Spark][Spark] and ....

###Apache Spark

[Apache Spark][Spark] is a framework for distributed in-memory data processing. It runs locally, on an in-house or commercial cloud environment.

[Spark]: https://spark.apache.org/
