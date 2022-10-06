ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.1"

lazy val root = (project in file("."))
  .settings(
    name := "l12-rdd-dtaframe-dataset"
  )

val sparkVersion = "3.2.2"
val postgresVersion = "42.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,

  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion % "runtime",
)