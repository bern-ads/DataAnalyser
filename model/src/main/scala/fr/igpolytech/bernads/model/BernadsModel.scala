package fr.igpolytech.bernads.model

import fr.igpolytech.bernads.cleanning.DataCleaner
import fr.igpolytech.bernads.runtime.BernadsApp
import fr.igpolytech.bernads.cleanning.Implicit._
import fr.igpolytech.bernads.runtime.Implicit._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.ml.linalg.Vector

class BernadsModel(dataPath: String, modelPath: String) extends BernadsApp {

  implicit val cleaner: DataCleaner = new ModelDataCleaner
  var result: DataFrame = _

  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[3]").config("spark.executor.memory", "15g")
  }

  override def run(): Unit = {
    readJson(dataPath) ~> cleanDataFrame ~> createModel ~> evaluateModel ~> saveModel
  }

  def createModel(dataFrame: DataFrame): RandomForestClassificationModel = {
    val selector = new ChiSqSelector()
      .setNumTopFeatures(6)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    result = selector.fit(dataFrame).transform(dataFrame)

    val rfc = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setMaxDepth(30)
      .setMaxBins(1116)
      .setNumTrees(50)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val paramGrid = new ParamGridBuilder()
      // .addGrid(rfc.numTrees, Array(50, 55, 60))
      .build()

    val cv = new CrossValidator()
      .setEstimator(rfc)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    cv.fit(result).bestModel.asInstanceOf[RandomForestClassificationModel]
  }

  def evaluateModel(model: RandomForestClassificationModel): RandomForestClassificationModel = {
    val predictions = model.transform(result)

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
      .rdd
      .map { row =>
        val prediction = if (row.getAs[Vector]("probability")(1) > 0.025) 1.0 else 0.0
        prediction -> row.getAs[Double]("label")
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
    model.save(modelPath)
  }

}
