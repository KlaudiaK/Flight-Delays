ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "FlightDelays",
    idePackagePrefix := Some("put.poznan.pl.klaudiak")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

val sparkVersion = "3.2.0"
libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "3.7.0",
  "org.apache.kafka" % "kafka-clients" % "3.2.0",
)
