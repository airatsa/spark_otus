ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.1"

lazy val root = (project in file("."))
  .settings(
    name := "l18-spark-testing"
  )

val sparkVersion = "3.2.2"
val scalaTestVersion = "3.2.1"
val postgresVersion = "42.5.0"
val testcontainersScalaVersion = "0.40.11"

Test / fork := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",

  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,

  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test",

  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion % "runtime",
)