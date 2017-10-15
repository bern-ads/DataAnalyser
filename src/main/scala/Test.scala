import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ChiSqSelector, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {

  def normalizeDataFrame(data: DataFrame) = {
    val toDouble = udf[Double, Boolean](b => if (b) 1.0 else 0.0)
    val clearOs = udf((os: String) => {
      if (os == null) "NULL"
      else
        os.toUpperCase match {
          case osName if osName.contains("WINDOW") => "WINDOWS"
          case osName => osName
        }
    })

    val df = data
      .withColumn("labelInt", toDouble(data("label")))
      .withColumn("osClear", clearOs(data("os")))

    val stringColumns = Array(
      "osClear",
      "media",
      "appOrSite",
      "interests",
      "city",
      "exchange",
      "publisher",
      "type",
      // "impid",
      "network"
    )
    val stringIndexers = stringColumns.map { name =>
      new StringIndexer()
        .setInputCol(name)
        .setOutputCol(s"${name}Indexed")
        .setHandleInvalid("keep")
        .fit(df)
    }
    val dataIndexed = stringIndexers.foldLeft(df) { (df, indexer) => indexer.transform(df) }

    val outColumns = Array(

    ) ++ stringColumns.map { name => s"${name}Indexed" }
    val vectorAssembler = new VectorAssembler()
      .setInputCols(outColumns)
      .setOutputCol("features")
    val dataAssembled = vectorAssembler.transform(dataIndexed)

    dataAssembled
      .select("labelInt", "features")
      .withColumnRenamed("labelInt", "label")
  }

  def example2(data: DataFrame): Unit = {
    val df = normalizeDataFrame(data)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(df)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(df)

    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxBins(500)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt))

//    val model = pipeline.fit(trainingData)
//
//    val predictions = model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    val paramGrid = new ParamGridBuilder().build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val model = cv.fit(trainingData).bestModel
    val predictions = model.transform(testData)

    val predictionAndLabels = predictions.select("prediction", "label")
      .rdd
      .map(row => (row.getAs[Double]("prediction"), row.getAs[Double]("label")))

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("Main method take one parameter: the path to the data file !")
    }

    val spark = SparkSession.builder
      .master("local")
      .appName("Bern-Ads DataAnalyser")
      .getOrCreate()

    val dataFrame = spark.read.json(args(0))
    val normalized = normalizeDataFrame(dataFrame)

    val selector = new ChiSqSelector()
      .setNumTopFeatures(7)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
    val result = selector.fit(normalized).transform(normalized)

    val Array(trainingData, testData) = result.randomSplit(Array(0.7, 0.3))

    val dt = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")
      //.setMaxDepth(30)
      //.setMaxBins(19158)
      //.setNumTrees(10)
      .setLayers(Array(7, 10, 10, 2))
      .setBlockSize(128)
      .setMaxIter(100)

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

    val model = cv.fit(result).bestModel//.asInstanceOf[MultilayerPerceptronClassifier]
    val predictions = model.transform(result)

    // println(model.toDebugString)

    val toInt = udf[Double, Double](b => if (b < 0.2) 0.0 else 1.0)

    val predictionAndLabels = predictions
      // .withColumn("predictionI", toInt(predictions("prediction")))
      .select("prediction", "label")
      .rdd
      .map(row => (row.getAs[Double]("prediction"), row.getAs[Double]("label")))

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.confusionMatrix)
    println(s"precision=${metrics.weightedPrecision}")
    println(s"recall=${metrics.weightedRecall}")
    println(s"tp=${metrics.weightedTruePositiveRate}")
    println(s"fp=${metrics.weightedFalsePositiveRate}")

//    val cleanPipeline = new Pipeline()
//      .setStages(
//        Array(
//          cleanString(fields),
//          normalizeString(fields),
//          compact(fields, "features"),
//          normalizeBool(Array("label"))
//        )
//      )

/*  // EXAMPLE 1

    val clearOs = udf((os: String) => {
      if (os == null) "NULL"
      else
        os.toUpperCase match {
          case osName if osName.contains("WINDOW") => "WINDOWS"
          case osName => osName
        }
    })

    val cleanedData = dataFrame
      .withColumn("osClear", clearOs($"os"))
      .select("osClear", "appOrSite", "media", "interests", "label")
      .withColumnRenamed("osClear", "os")
      .rdd
      .map { row => PersonModel(row).toLabeledPoint }

    val Array(trainingData, testData) = cleanedData.randomSplit(Array(0.7, 0.3))
    trainingData.cache()
    testData.cache()

    val numClasses = 2
    val impurity = "entropy"
    val maxDepth = 20
    val maxBins = 24
    val tree = DecisionTree.trainClassifier(
      trainingData,
      numClasses,
      PersonModel.categoricalFeaturesInfo(),
      impurity,
      maxDepth,
      maxBins
    )

    println(tree.toDebugString)
    println("(tp, tn, fp, fn)")
    println(Stats.confusionMatrix(tree, testData))

    testData.take(15).foreach {
      x => println(s"Predicted: ${tree.predict(x.features)}, actual value: ${x.label}")
    }*/
  }

}
