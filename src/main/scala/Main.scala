import fr.igpolytech.berdads.PersonModel
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("Main method take one parameter: the path to the data file !")
    }

    val spark = SparkSession.builder
      .master("local")
      .appName("Bern-Ads DataAnalyser")
      .getOrCreate()
    import spark.implicits._

    val dataFrame = spark.read.json(args(0))

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
      .select("osClear", "appOrSite", "media", "label")
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

    testData.take(15).foreach {
      x => println(s"Predicted: ${tree.predict(x.features)}, actual value: ${x.label}")
    }
  }

}
