/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import sbt.Resolver

val scalaVersionParameterOption = Option(System.getProperty("scala.version"))
val sparkVersionParameterOption = Option(System.getProperty("spark.version"))

val sversion = scalaVersionParameterOption match {
  case Some(x) => x
  case None => "2.11.2"
}

val sparkVersion = sparkVersionParameterOption match {
  case Some(x) => x
  case None => "2.0.0"
}

assemblyJarName in assembly := "SciSpark.jar"

name := "SciSpark"

organization := "org.dia"

version := "1.0"

// Default is 2.10.6
scalaVersion := sversion

scalacOptions := Seq("-feature", "-deprecation")

mainClass in (Compile, packageBin) := Some("org.dia.apps.MCSApp")

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
  "org.scalatest" %% "scalatest" % "3.0.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  // Math Libraries
  // "org.jblas" % "jblas" % "1.2.3",
  // other dependencies here
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.json4s" %% "json4s-native" % "3.2.11",
  // native libraries greatly improve performance, but increase jar sizes.
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  // Nd4j scala api with netlib-blas backend
  // "org.nd4j" % "nd4s_2.10" % "0.5.0",
  "org.nd4j" % "nd4j-native-platform" % "0.5.0",
  "org.nd4j" %% "nd4j-kryo" % "0.5.0",
  "edu.ucar" % "opendap" % "4.6.0",
  "joda-time" % "joda-time" % "2.8.1",
  "org.joda" % "joda-convert" % "1.8.1",
  "com.joestelmach" % "natty" % "0.11",
  "edu.ucar" % "cdm" % "4.6.0"
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

