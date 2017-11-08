package fr.igpolytech.bernads.cleanning

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

trait DataCleaner {

  def clean(dataFrame: DataFrame): DataFrame

}

/**
  * Helper methods to manipulate more easily a dataframe object.
  */
case class DataCleanerHelper(dataFrame: DataFrame) {

  def normalizeLabel(inColumn: String, outColumn: String): DataCleanerHelper = {
    this.cleanInput(inColumn, outColumn, udf[Double, Boolean](b => if (b) 1.0 else 0.0))
  }

  def cleanInput(inColumn: String, outColumn: String, f: UserDefinedFunction): DataCleanerHelper = {
    DataCleanerHelper(
      dataFrame
        .withColumn(outColumn, f(dataFrame(inColumn)))
    )
  }

  def normalizeStringInputs(inColumns: Array[String], outSuffix: String, nullPolicy: Map[String, String]): DataCleanerHelper = {
    val stringIndexers = inColumns.map { name =>
      new StringIndexer()
        .setInputCol(name)
        .setOutputCol(s"$name$outSuffix")
        .setHandleInvalid(nullPolicy(name))
        .fit(dataFrame)
    }
    val dataIndexed = stringIndexers.foldLeft(dataFrame) { (df, indexer) => indexer.transform(df) }
    DataCleanerHelper(dataIndexed)
  }

  def encodeStringInputs(inColumns: Array[String], outSuffix: String): DataCleanerHelper = {
    val stringEncoders = inColumns.map { name =>
      new OneHotEncoder()
        .setInputCol(name)
        .setOutputCol(s"$name$outSuffix")
    }
    val dataIndexed = stringEncoders.foldLeft(dataFrame) { (df, indexer) => indexer.transform(df) }
    DataCleanerHelper(dataIndexed)
  }

  def compact(inColumns: Array[String], outColumn: String): DataCleanerHelper = {
    val vectorAssembler = new VectorAssembler()
      .setInputCols(inColumns)
      .setOutputCol(outColumn)
    DataCleanerHelper(vectorAssembler.transform(dataFrame))
  }

}
