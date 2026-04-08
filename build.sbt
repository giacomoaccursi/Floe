organization := "io.github.giacomoaccursi"

name := "spark-etl-framework"

// Version is derived from git tags by sbt-dynver
ThisBuild / dynverSeparator := "-"

scalaVersion := "2.12.18"

// Publish to GitHub Packages
publishTo := Some(
  "GitHub Packages" at s"https://maven.pkg.github.com/${sys.env.getOrElse("GITHUB_REPOSITORY", "giacomoaccursi/spark-etl-framework")}"
)
credentials ++= sys.env.get("GITHUB_TOKEN").map { token =>
  Credentials("GitHub Package Registry", "maven.pkg.github.com", "_", token)
}.toSeq

val sparkVersion = "3.5.8"
val icebergVersion = "1.10.1"
val scalaTestVersion = "3.2.19"
val scalaCheckVersion = "1.19.0"
val json4sVersion = "3.7.0-M11"

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // Iceberg table format
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion,

  // Configuration loading with YAML support (supports case class defaults natively)
  "com.github.pureconfig" %% "pureconfig" % "0.17.4",
  "com.github.pureconfig" %% "pureconfig-yaml" % "0.17.4",

  // JSON serialization for metadata
  "org.json4s" %% "json4s-jackson" % json4sVersion,

  // Logging
  "org.slf4j" % "slf4j-api" % "2.0.17",
  "ch.qos.logback" % "logback-classic" % "1.4.14",

  // Testing
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
  "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % Test,
  "com.h2database" % "h2" % "2.2.224" % Test
)

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:privates"
)

// Test options
Test / parallelExecution := false
Test / fork := true
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")
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
