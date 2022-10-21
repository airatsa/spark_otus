ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "l16-spark-ml-streaming"
  )

val sparkVersion = "3.1.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "2.6.0" % sparkVersion,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.12.4",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.12.4",
  "org.apache.commons" % "commons-csv" % "1.9.0",
  "com.github.scopt" % "scopt_2.12" % "4.1.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0",
)

//assembly / assemblyMergeStrategy := {
//  case m if m.toLowerCase.endsWith("manifest.mf")       => MergeStrategy.discard
//  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")   => MergeStrategy.discard
//  case "reference.conf"                                 => MergeStrategy.concat
//  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
//  case _                                                => MergeStrategy.first
//}