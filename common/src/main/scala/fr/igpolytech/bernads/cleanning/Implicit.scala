package fr.igpolytech.bernads.cleanning

import org.apache.spark.sql.DataFrame

object Implicit {

  implicit class DataFrameWithCleaner(dataFrame: DataFrame) {
    def clean(f: (DataCleaner) => DataCleaner): DataFrame = {
      f(DataCleaner(dataFrame)).dataFrame
    }
  }

}
