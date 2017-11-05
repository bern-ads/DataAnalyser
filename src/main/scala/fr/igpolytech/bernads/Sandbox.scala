package fr.igpolytech.bernads

import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession.Builder
import org.apache.log4j.Logger
import org.apache.log4j.Level


import org.apache.spark.sql.{DataFrame}



class Sandbox extends BernadsApp {

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
      .setNumTopFeatures(6)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)

    val splits = result.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    val paramGrid = new ParamGridBuilder()
      .build()

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setMaxDepth(15)
      .setMaxBins(1000)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")


    //println(evaluator.explainParams())

    val cv = new CrossValidator()
      .setEstimator(dt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val model = cv.fit(trainingData).bestModel.asInstanceOf[DecisionTreeClassificationModel]
    val predictions = model.transform(testData)

    println(model.toDebugString)
    //println(model.explainParams())

    val predictionAndLabels = predictions
      .select("prediction", "label")
      .rdd
      .map { row =>
        row.getAs[Double]("prediction") -> row.getAs[Double]("label")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    //println(s"recall=${metrics.weightedRecall}")
    println(s"true positive rate = ${metrics.weightedTruePositiveRate}")
    println(s"false positive rate = ${metrics.weightedFalsePositiveRate}")
  }

}
