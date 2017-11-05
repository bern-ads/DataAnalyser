package fr.igpolytech.bernads

import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{GBTClassificationModel, RandomForestClassificationModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.ml.classification.{GBTClassifier,RandomForestClassifier, MultilayerPerceptronClassifier}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.linalg.Vector

class Sandbox3 extends BernadsApp {

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
    val splits = result.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))

    val gbt = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setLayers(Array[Int](6, 80, 80, 2))
      .setBlockSize(128)
      //.setMaxBins(1000)
      .setMaxIter(100)
    //.setNumTrees(50)

    println(gbt.explainParams())


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")


    val paramGrid = new ParamGridBuilder()
      .build()

    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val model = cv.fit(trainingData) //.bestModel.asInstanceOf[MultilayerPerceptronClassifier]
    val predictions = model.transform(testData)
    //println(model.toDebugString)
    //println(model.explainParams())
    val predictionAndLabels = predictions
      .select("prediction", "label")
      .rdd
      .map { row =>
        //val prediction = if (row.getAs[Vector]("probability")(1) > 0.025) 1.0 else 0.0
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
