ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.1"

lazy val root = (project in file("."))
  .settings(
    name := "l17-kafka"
  )

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "org.apache.commons" % "commons-csv" % "1.9.0",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.13.14",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.13.14"
)