name := "SciSparkTestExperiments"

version := "1.0"

//scalaVersion := "2.11.6"
scalaVersion := "2.10.4"

mainClass in Compile := Some("Main")

resolvers ++= Seq(
  //other resolvers here
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.0",
  "org.jblas" % "jblas" % "1.2.3",
  "edu.ucar" % "opendap" % "2.2.2",
  "edu.ucar" % "netcdf" % "4.2.20"
)
