import fr.igpolytech.bernads.runtime.{BernadsApp, BernadsDataCleaner}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame

class Sandbox2 extends BernadsApp {

  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[3]").config("spark.executor.memory", "15g")
  }

  override def run(args: Array[String]): Unit = {
    import fr.igpolytech.bernads.cleanning.Implicit._

    def balanceDataset(dataset: DataFrame): DataFrame = {
      import org.apache.spark.sql.functions._

      // Re-balancing (weighting) of records to be used in the logistic loss objective function
      val numNegatives = dataset.filter(dataset("label") === 0).count
      val datasetSize = dataset.count
      val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

      val calculateWeights = udf { d: Double =>
        if (d == 0.0) {
          1 * balancingRatio
        }
        else {
          (1 * (1.0 - balancingRatio))
        }
      }

      val weightedDataset = dataset.withColumn("classWeightCol", calculateWeights(dataset("labelInt")))
      weightedDataset
    }

    val df = balanceDataset({
      readJson(args(0)).clean {
        BernadsDataCleaner.cleaner
      }
    })

    val selector = new ChiSqSelector()
      .setNumTopFeatures(6)
      .setFeaturesCol("features")
      .setLabelCol("labelInt")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(df).transform(df)

    val splits = result.randomSplit(Array(0.75, 0.25))
    val (trainingData, testData) = (splits(0), splits(1))

    val gbt = new LogisticRegression()
      .setLabelCol("labelInt")
      .setFeaturesCol("selectedFeatures")
      .setWeightCol("classWeightCol")

    //println(gbt.explainParams())

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("labelInt")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.regParam, Array(0.01, 0.15, 0.30, 0.45))
      .addGrid(gbt.elasticNetParam, Array(0.7, 0.8, 0.9))
      .addGrid(gbt.maxIter, Array(5, 10, 20))
      .build()

    val cv = new CrossValidator()
      .setEstimator(gbt)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val model = cv.fit(trainingData).bestModel
    val predictions = model.transform(result)
    //println(model.toDebugString)
    //println(model.explainParams())
    import org.apache.spark.sql.functions._
    val getp0 = udf((v: org.apache.spark.ml.linalg.Vector) => v(0))
    val getp1 = udf((v: org.apache.spark.ml.linalg.Vector) => v(1))
    predictions
      .withColumn("p0", getp0(predictions("probability")))
      .withColumn("p1", getp1(predictions("probability")))
      .select("prediction", "labelInt", "probability", "p0", "p1")
      .groupBy("prediction", "labelInt")
      .avg("p0", "p1")
      .show(100, false)

    val predictionAndLabels = predictions
      .select("prediction", "labelInt", "probability")
      .rdd
      .map { row =>
        // val prediction = row.getAs[Double]("prediction")
          // if (row.getAs[Vector]("probability")(1) > 0.04) 1.0 else 0.0
        val prediction = if (row.getAs[Vector]("probability")(1) > 0.025) 1.0 else 0.0
        prediction -> row.getAs[Double]("labelInt")
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    println(s"recall=${metrics.weightedRecall}")
    println(s"true positive rate = ${metrics.weightedTruePositiveRate}")
    println(s"false positive rate = ${metrics.weightedFalsePositiveRate}")
  }
}