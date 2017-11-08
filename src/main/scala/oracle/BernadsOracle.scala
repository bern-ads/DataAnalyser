package oracle

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import fr.igpolytech.bernads.cleanning.DataCleaner
import fr.igpolytech.bernads.runtyme.{BernadsApp, BernadsDataCleaner}
import fr.igpolytech.bernads.cleanning.Implicit._
import fr.igpolytech.bernads.runtyme.Implicit._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

class BernadsOracle(dataPath: String, selectorPath: String, modelPath: String, resultPath: String) extends BernadsApp {

  implicit val cleaner: DataCleaner = new BernadsDataCleaner
  implicit lazy val selector: ChiSqSelectorModel = ChiSqSelectorModel.load(selectorPath)
  implicit lazy val model: RandomForestClassificationModel = RandomForestClassificationModel.load(modelPath)

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
    readJson(dataPath) ~> cleanDataFrame ~> applySelection ~> applyModel ~> saveResult
  }

  def applyModel(dataFrame: DataFrame)(implicit model: RandomForestClassificationModel): DataFrame = {
    val timestamp: Long = System.currentTimeMillis()/1000
    println("start apply model "+ timestamp)
    //val splitedDataFrame = dataFrame.randomSplit(Array(0.90,0.10))
    val toBool = udf((prediction: Double) => if (prediction == 0.0) false else true)
    val predictions = model.transform(dataFrame)
    val finalPredictions = predictions.withColumn("predicted", binarize(predictions("probability")))
    val timestamp1: Long = System.currentTimeMillis()/1000
    println("end apply model "+timestamp1)
    val intervalTime: Long = timestamp1 - timestamp
    println(intervalTime)
    finalPredictions.withColumn("predictedBool", toBool(finalPredictions("predicted")))
  }

  def applySelection(dataFrame: DataFrame)(implicit selector: ChiSqSelectorModel): DataFrame = {
    selector.transform(dataFrame)
  }

  def saveResult(dataFrame: DataFrame): Unit = {
    println("start save result")
    val tmpDir = "result.tmp"

    dataFrame
      .select("predictedBool", dataFrame.select(
        "appOrSite",
        "bidfloor",
        "city",
        "exchange",
        "impid",
        "interests",
        "media",
        "network",
        "os",
        "publisher",
        "timestamp",
        "type",
        "user"
      ).schema.fieldNames: _*) // Add the predicted field in first col
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv(tmpDir)

    val tmpDirectory = new File(tmpDir)
    val tmpFiles = tmpDirectory.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv"))
    if (tmpFiles.nonEmpty) {
      Files.move(
        tmpFiles.head.toPath,
        Paths.get(resultPath),
        StandardCopyOption.REPLACE_EXISTING
      )
      FileUtils.deleteDirectory(tmpDirectory)
    }
    val timestamp2: Long = System.currentTimeMillis()/1000
    println("end save resultht "+ timestamp2)
  }

}