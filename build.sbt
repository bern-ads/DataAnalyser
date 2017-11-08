name := "DataAnalyser"

version := "1.0"

scalaVersion := "2.11.11"
//logLevel := Level.Warn

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

val sparkVersion = "2.2.0"
val commonLibs = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.databricks" %% "spark-csv" % "1.5.0"
)

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= commonLibs
  )