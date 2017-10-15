package fr.igpolytech.bernads.runtime

import org.apache.spark.sql.{DataFrame, SparkSession}

trait BernadsApp {

  val sparkSession = {
    this.configure(
      SparkSession.builder()
      .appName("Bernads Application")
    ).getOrCreate()
  }

  def configure(builder: SparkSession.Builder): SparkSession.Builder

  def readJson(path: String): DataFrame = {
    sparkSession.read.json(path)
  }

  def run(args: Array[String]): Unit

}

object BernadsApp {

  def apply(app: => BernadsApp, args: Array[String]): Unit = {
    app.run(args)
  }

}
