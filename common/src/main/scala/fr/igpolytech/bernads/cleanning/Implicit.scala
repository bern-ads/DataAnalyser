package fr.igpolytech.bernads.cleanning

import org.apache.spark.sql.DataFrame

object Implicit {

  def cleanDataFrame(dataFrame: DataFrame)(implicit dataCleaner: DataCleaner): DataFrame = {
    dataCleaner.clean(dataFrame)
  }

}
