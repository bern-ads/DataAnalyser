package model

import fr.igpolytech.bernads.cleanning.{DataCleaner, DataCleanerHelper}
import fr.igpolytech.bernads.runtyme.BernadsDataCleaner
import org.apache.spark.sql.DataFrame

class ModelDataCleaner extends DataCleaner {

  val commonCleaner = new BernadsDataCleaner

  /**
    * Convert the original dataframe to a dataframe with two columns: label and features
    */
  override def clean(dataFrame: DataFrame) = {
    val commonCleannedDataFrame = commonCleaner.clean(dataFrame)
    DataCleanerHelper(commonCleannedDataFrame)
      .normalizeLabel("label", "labelInt")
      .dataFrame
      .select("labelInt", "features")
      .withColumnRenamed("labelInt", "label")
  }

}
