import sbt.Resolver

assemblyJarName in assembly := "SciSpark.jar"

name := "SciSparkTestExperiments"

version := "1.0"

scalaVersion := "2.10.6"

scalacOptions := Seq("-feature", "-deprecation")

mainClass in Compile := Some("org.dia.algorithms.mcc.MainDistGraphMCC")

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.file("Local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns),
  "maven Repository" at "http://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/"
)

/**
 * unmanagedBase and unmanagedJars are to provide a way to include custom jars.
 * This is specifically helpful for nd4s builds.
 */
unmanagedBase := baseDirectory.value / "lib"

unmanagedJars in Compile := (baseDirectory.value ** "*.jar").classpath

val buildSettings = Defaults.coreDefaultSettings ++ Seq {
  javaOptions += "-Xms4000M -Xmx5024M -Xss1M -XX:MaxPermSize=256M"
}

/**
 * Prevents multiple SparkContexts from being launched
 */
parallelExecution in Test := false

test in assembly := {}

/**
 * There are conflicting slf4j versions between spark and nd4j. Due to the
 * recency of Nd4j and it's development speed it is using the latest slf4j version.
 */
classpathTypes += "maven-plugin"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0-M15",
  "org.apache.spark" % "spark-core_2.10" % "1.6.0" exclude("org.slf4j", "slf4j-api"),
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.0",
  //Math Libraries
  //"org.jblas" % "jblas" % "1.2.3",
  // other dependencies here
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.json4s" %% "json4s-native" % "3.2.11",
  // native libraries greatly improve performance, but increase jar sizes.
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  // Nd4j scala api with netlib-blas backend
  "org.nd4j" % "nd4s_2.10" % "0.4-rc3.8",
  "org.nd4j" % "nd4j-x86" % "0.4-rc3.8",
  "edu.ucar" % "opendap" % "4.6.0",
  "joda-time" % "joda-time" % "2.8.1",
  "org.joda" % "joda-convert" % "1.8.1",
  "com.joestelmach" % "natty" % "0.11",
  "edu.ucar" % "cdm" % "4.6.6"
)

assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
  case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains("org/cyberneko/html") => MergeStrategy.first
  case x if x.contains("SingleThreadModel.class") => MergeStrategy.first
  case x if x.contains("javax.servlet") => MergeStrategy.first
  case x if x.contains("org.eclipse") => MergeStrategy.first
  case x if x.contains("org.apache") => MergeStrategy.first
  case x if x.contains("org.slf4j") => MergeStrategy.first
  case x if x.endsWith("reference.conf") => MergeStrategy.concat
  case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.last // For Log$Logger.class
  case x => MergeStrategy.first
}

