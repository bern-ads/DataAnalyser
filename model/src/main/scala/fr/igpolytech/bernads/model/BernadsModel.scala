package fr.igpolytech.bernads.model

import fr.igpolytech.bernads.cleanning.DataCleaner
import fr.igpolytech.bernads.runtime.BernadsApp
import fr.igpolytech.bernads.cleanning.Implicit._
import fr.igpolytech.bernads.runtime.Implicit._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.udf

import scala.util.Random

class BernadsModel(dataPath: String, selectorPath: String, modelPath: String) extends BernadsApp {

  implicit val cleaner: DataCleaner = new ModelDataCleaner
  var result: Array[Dataset[Row]] = _

  /**
    * Configure the Spark context with given SessionBuilder.
    */
  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[*]").config("spark.executor.memory", "12g")
  }

  /**
    * Run the Spark application.
    */
  override def run(): Unit = {
    readJson(dataPath) ~> cleanDataFrame ~> createModel ~> evaluateModel ~> saveModel
  }

  def createModel(dataFrame: DataFrame): RandomForestClassificationModel = {
    val udfRandGen = udf((label: Double) => if (label == 1.0) new Random().nextInt(12) else -1)
    val finalData = dataFrame
      .withColumn("randIndex", udfRandGen(dataFrame("label")))
      .filter("randIndex < 1")
      .drop("randIndex")

    val selector = new ChiSqSelector()
      .setNumTopFeatures(6)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
      .fit(finalData)
    selector.write.overwrite().save(selectorPath)

    val selected = selector.transform(dataFrame)
    result = selected.randomSplit(Array(0.75, 0.25), seed = 1234)
    val trainingData = result(0)

    val rfc = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setMaxDepth(30)
      .setMaxBins(7696)
      .setNumTrees(55)

    rfc.fit(trainingData)
  }

  def evaluateModel(model: RandomForestClassificationModel): RandomForestClassificationModel = {
    val predictions = model.transform(result(1))

    import org.apache.spark.sql.functions._
    val getp0 = udf((v: org.apache.spark.ml.linalg.Vector) => v(0))
    val getp1 = udf((v: org.apache.spark.ml.linalg.Vector) => v(1))
    predictions
      .withColumn("p0", getp0(predictions("probability")))
      .withColumn("p1", getp1(predictions("probability")))
      .select("prediction", "label", "probability", "p0", "p1")
      .groupBy("prediction", "label")
      .avg("p0", "p1")
      .show(100, false)

    predictions
      .withColumn("p0", getp0(predictions("probability")))
      .withColumn("p1", getp1(predictions("probability")))
      .select("prediction", "label", "probability", "p0", "p1")
      .groupBy("prediction", "label")
      .agg(stddev("p0"), stddev("p1"))
      .show(100, false)

    predictions
      .withColumn("p0", getp0(predictions("probability")))
      .withColumn("p1", getp1(predictions("probability")))
      .select("prediction", "label", "probability", "p0", "p1")
      .groupBy("prediction", "label")
      .count()
      .show(100, false)

    val predictionAndLabels = predictions
      .select("prediction", "label", "probability")
      .withColumn("predicted", binarize(predictions("probability")))
      .rdd
      .map { row =>
        row.getAs[Double]("predicted") -> row.getAs[Double]("label")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    println(s"recall=${metrics.weightedRecall}")
    println(s"true positive rate = ${metrics.weightedTruePositiveRate}")
    println(s"false positive rate = ${metrics.weightedFalsePositiveRate}")

    model
  }

  def saveModel(model: MLWritable): Unit = {
    model.write.overwrite().save(modelPath)
  }

}