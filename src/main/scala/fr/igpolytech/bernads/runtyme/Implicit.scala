package fr.igpolytech.bernads.runtyme

import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.DataFrame

object Implicit {

  /**
    * Pipeline operator to have an aesthetic DSL in BernadsApp run
    * (like the '|' in bash)
    */
  implicit class WithPipeline[K](source: K) {
    def ~>[T](f: (K) => T): T = {
      f(source)
    }
  }

}

