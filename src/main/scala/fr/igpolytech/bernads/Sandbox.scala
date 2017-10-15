package fr.igpolytech.bernads

import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession.Builder

class Sandbox extends BernadsApp {

  override def configure(builder: Builder): Builder = {
    builder.master("local")
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

    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setMaxDepth(30)
      .setMaxBins(19158)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val paramGrid = new ParamGridBuilder().build()

    val cv = new CrossValidator()
      .setEstimator(dt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val model = cv.fit(result).bestModel.asInstanceOf[DecisionTreeClassificationModel]
    val predictions = model.transform(result)

    println(model.toDebugString)

    val predictionAndLabels = predictions
      .select("prediction", "label")
      .rdd
      .map { row =>
        row.getAs[Double]("prediction") -> row.getAs[Double]("label")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    println(s"recall=${metrics.weightedRecall}")
    println(s"tp=${metrics.weightedTruePositiveRate}")
    println(s"fp=${metrics.weightedFalsePositiveRate}")
  }

}
