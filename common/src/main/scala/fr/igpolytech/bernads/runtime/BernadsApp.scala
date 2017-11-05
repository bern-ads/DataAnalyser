package fr.igpolytech.bernads.runtime

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Wrapper for Spark applications.
  * Allow to configure and to run Spark application more easily (with a configure and a run method).
  */
trait BernadsApp {

  val sparkSession = {
    this.configure(
      SparkSession.builder()
      .appName("Bernads Application")
    ).getOrCreate()
  }

  /**
    * Configure the Spark context with given SessionBuilder.
    */
  def configure(builder: SparkSession.Builder): SparkSession.Builder

  def readJson(path: String): DataFrame = {
    sparkSession.read.json(path)
  }

  /**
    * Run the Spark application.
    */
  def run(): Unit

}

object BernadsApp {

  def apply(app: => BernadsApp): Unit = {
    app.run()
  }

}
