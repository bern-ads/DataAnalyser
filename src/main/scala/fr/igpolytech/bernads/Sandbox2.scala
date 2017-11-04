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

class Sandbox2 extends BernadsApp {

  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[*]").config("spark.executor.memory", "15g")
  }

  def generateModel(pathToBuild: String): RandomForestClassificationModel = {
    import fr.igpolytech.bernads.cleanning.Implicit._

    val df = {
      readJson(pathToBuild).clean {
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

    val rbt = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      .setMaxDepth(10)
      .setMaxBins(1000)
      //.setMaxIter(30)
      .setNumTrees(50)


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")


    val paramGrid = new ParamGridBuilder()
      .build()

    val cv = new CrossValidator()
      .setEstimator(rbt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    cv.fit(trainingData).bestModel.asInstanceOf[RandomForestClassificationModel]
  }

  def saveModel(pathToBuild: String, pathToSave: String): Unit = {
    generateModel(pathToBuild).save(pathToSave)
  }

  override def run(args: Array[String]): Unit = {
    val model = generateModel(args(0))
    /*
    val predictions = model.transform(result)
    //println(model.toDebugString)
    //println(model.explainParams())
    val predictionAndLabels = predictions
      .select("prediction", "label", "probability")
      .rdd
      .map { row =>
        val prediction = if (row.getAs[Vector]("probability")(1) > 0.015) 1.0 else 0.0
        prediction -> row.getAs[Double]("label")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"accuracy = ${metrics.accuracy}")
    println(s"precision = ${metrics.weightedPrecision}")
    //println(s"recall=${metrics.weightedRecall}")
    println(s"true positive rate = ${metrics.weightedTruePositiveRate}")
    println(s"false positive rate = ${metrics.weightedFalsePositiveRate}")
    */
  }

}