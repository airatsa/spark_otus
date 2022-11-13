ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.1"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-producer"
  )

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.github.scopt" % "scopt_2.13" % "4.1.0",
)