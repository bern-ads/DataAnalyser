package fr.igpolytech.bernads.oracle

import fr.igpolytech.bernads.cleanning.DataCleaner
import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import fr.igpolytech.bernads.cleanning.Implicit._
import fr.igpolytech.bernads.runtime.Implicit._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}

class BernadsOracle(dataPath: String, selectorPath: String, modelPath: String, resultPath: String) extends BernadsApp {

  implicit val cleaner: DataCleaner = new BernadsDataCleaner
  implicit lazy val model: RandomForestClassificationModel = RandomForestClassificationModel.load(modelPath)
  implicit val selector : ChiSqSelector = ChiSqSelector.load(selectorPath)
  /**
    * Configure the Spark context with given SessionBuilder.
    */
  override def configure(builder: SparkSession.Builder): SparkSession.Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[*]").config("spark.executor.memory", "15g")
  }

  /**
    * Run the Spark application.
    */
  override def run(): Unit = {
    readJson(dataPath) ~> cleanDataFrame ~> applyModel ~> saveResult
  }

  def applyModel(dataFrame: DataFrame)(implicit model: RandomForestClassificationModel): DataFrame = {
    // selector.
    val predictions = model.transform(dataFrame)
    predictions.withColumn("predicted", binarize(predictions("probability")))
  }

  def saveResult(dataFrame: DataFrame): Unit = {
    dataFrame
      .select("predicted", dataFrame.schema.fieldNames: _*) // Add the predicted field in first col
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv(resultPath)

    /* Travail de théo
    def write(filePath: String)(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .coalesce(1) // Merge all partitions to write just one csv file
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .save("tmp")

    // Move the real csv file inside `tmp` to the app's root
    val tmpDirectory = new File("tmp")
    if (tmpDirectory.exists && tmpDirectory.isDirectory) {
      val tmpFiles = tmpDirectory.listFiles.filter(_.isFile).toList.filter(_.getName.endsWith(".csv"))
      if (tmpFiles.nonEmpty) {
        val csvTmpPath = tmpFiles.head.toPath
        val finalPath = new File(filePath).toPath
        Files.move(csvTmpPath, finalPath, StandardCopyOption.REPLACE_EXISTING)
      }
    }

    dataFrame
     */
  }

}
