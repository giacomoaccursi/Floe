name := "spark-etl-framework"

version := "0.1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.8"
val scalaTestVersion = "3.2.19"
val scalaCheckVersion = "1.19.0"
val circeVersion = "0.14.15"

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // YAML parsing
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-yaml" % "1.15.0",

  // Logging
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "ch.qos.logback" % "logback-classic" % "1.4.14",

  // Testing
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % Test
)

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint"
)

// Test options
Test / parallelExecution := false
Test / fork := true
Test / javaOptions ++= Seq(
  "-Xmx2G",
  // Java 17+ module access for Spark
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
