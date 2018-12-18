name := "gtw-attribution"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  // "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  // "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  // "org.apache.spark" %% "spark-graphx" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-graphx" % "2.4.0",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "org.apache.kafka" %% "kafka" % "1.1.0",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.4"
)
