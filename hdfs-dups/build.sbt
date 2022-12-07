ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.1"

lazy val root = (project in file("."))
  .settings(
    name := "hdfs-dups"
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.3.2",
  "com.github.scopt" % "scopt_2.13" % "4.1.0"
)
