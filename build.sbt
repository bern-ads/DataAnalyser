name := "DataAnalyser"

version := "1.0"

scalaVersion := "2.10.5"
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
  .aggregate(common)
  .dependsOn(common)
  .settings(
    libraryDependencies ++= commonLibs
  )

lazy val model = project
  .aggregate(common)
  .dependsOn(common)
  .settings(
    libraryDependencies ++= commonLibs
  )

lazy val oracle = project
  .aggregate(common)
  .dependsOn(common)
  .settings(
    libraryDependencies ++= commonLibs
  )

lazy val common = project
  .settings(
    libraryDependencies ++= commonLibs
  )
