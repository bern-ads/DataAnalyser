import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, GBTClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
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
      .setNumTopFeatures(6)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)
    val splits = result.randomSplit(Array(0.75, 0.25))
    val (trainingData, testData) = (splits(0), splits(1))

    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(10)
      .setMaxBins(7696)
      .setMaxIter(10)

    //println(gbt.explainParams())

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxDepth, Array(1, 5, 10, 20, 30))
      .addGrid(gbt.maxBins, Array(7696, 14000, 24000))
      .addGrid(gbt.maxIter, Array(2, 5, 10))
      .build()

    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val model = cv.fit(trainingData).bestModel.asInstanceOf[GBTClassificationModel]
    val predictions = model.transform(testData)

    println(model.toDebugString)
    //println(model.explainParams())
    val predictionAndLabels = predictions
      .select("prediction", "label", "probability")
      .rdd
      .map { row =>
        val prediction = row.getAs[Double]("prediction")
          // if (row.getAs[Vector]("probability")(1) > 0.04) 1.0 else 0.0
        prediction -> row.getAs[Double]("label")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    println(s"recall=${metrics.weightedRecall}")
    println(s"true positive rate = ${metrics.weightedTruePositiveRate}")
    println(s"false positive rate = ${metrics.weightedFalsePositiveRate}")
  }
}