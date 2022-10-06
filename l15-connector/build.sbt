ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.1"

lazy val root = (project in file("."))
  .settings(
    name := "l15-connector"
  )

val testcontainersScalaVersion = "0.40.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.postgresql" % "postgresql" % "42.5.0",

  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test",
)

