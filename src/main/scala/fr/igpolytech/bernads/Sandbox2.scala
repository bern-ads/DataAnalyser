package fr.igpolytech.bernads

import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession.Builder
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector

class Sandbox2 extends BernadsApp {

  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[*]").config("spark.executor.memory", "15g")
  }

  override def run(args: Array[String]): Unit = {
    import fr.igpolytech.bernads.cleanning.Implicit._

    val df = {
      readJson(args(0)).clean {
        BernadsDataCleaner.cleaner
      }
        .select("labelInt", "features")
        .withColumnRenamed("labelInt", "label")
    }

    val selector = new ChiSqSelector()
      .setNumTopFeatures(7)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)

    val splits = result.randomSplit(Array(0.75, 0.25), seed = 200)
    val (trainingData, testData) = (splits(0), splits(1))

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setMaxDepth(30)
      .setMaxBins(618)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(1, 5, 10, 20, 30))
      .addGrid(dt.maxBins, Array(618, 1200, 2400))
      .build()

    val cv = new CrossValidator()
      .setEstimator(dt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val model = cv.fit(trainingData).bestModel
    val predictions = model.transform(testData)

    predictions
      .select("prediction", "label", "probability")
      .orderBy(desc("label"))
      .show(500, false)

    predictions
      .select("prediction", "label", "probability")
      .orderBy(asc("label"))
      .show(500, false)

    val predictionAndLabels = predictions
      .select("prediction", "label", "probability")
      .rdd
      .map { row =>
        val prediction = row.getAs[Double]("prediction")
         // if (row.getAs[Vector]("probability")(1) > 0.008) 1.0 else 0.0
        prediction -> row.getAs[Double]("label")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    println(s"recall=${metrics.weightedRecall}")
    println(s"tp=${metrics.weightedTruePositiveRate}")
    println(s"fp=${metrics.weightedFalsePositiveRate}")
  }

}
