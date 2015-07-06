name := "SciSparkTestExperiments"

version := "1.0"

//scalaVersion := "2.11.6"
scalaVersion := "2.10.4"

mainClass in Compile := Some("org.dia.n.MainNd4j")

resolvers ++= Seq(
  Resolver.mavenLocal
)

val buildSettings = Defaults.defaultSettings ++ Seq {
  javaOptions += "-Xms4000M -Xmx5024M -Xss1M -XX:MaxPermSize=256M"
}

/**
 * There are conflicting slf4j versions between spark and nd4j. Due to the
 * recency of Nd4j and it's development speed it is using the latest slf4j version.
 */
classpathTypes += "maven-plugin"
libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-SNAP4",
  "org.apache.spark" % "spark-core_2.10" % "1.3.0" exclude("org.slf4j" , "slf4j-api"),
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.0",
  //Math Libraries
  "org.jblas" % "jblas" % "1.2.3",
  // other dependencies here
  "org.scalanlp" %% "breeze" % "0.11.2",
  // native libraries are not included by default. add this if you want them (as of 0.7)
  //"org.nd4j" % "org" % "0.0.3.5.5.5",
  // native libraries greatly improve performance, but increase jar sizes.
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  // Nd4j scala api with netlib-blas backend
  //"org.nd4j" % "nd4j-api" % "0.0.3.5.5.5",
  "org.nd4j" % "nd4j-scala-api" % "0.0.3.5.5.6-SNAPSHOT",
  "org.nd4j" % "nd4j-x86" % "0.0.3.5.5.6-SNAPSHOT",
  //"org.nd4j" % "nd4j-netlib-blas" % "0.0.3.5.5.6-SNAPSHOT",
  //"org.nd4j" % "org" % "0.0.3.5.5.6-SNAPSHOT",
  //"org.projectlombok" % "lombok" % "1.16.4",
  "edu.ucar" % "opendap" % "2.2.2",
  "joda-time" % "joda-time" % "2.8.1",
  "edu.ucar" % "netcdf" % "4.2.20"
)
