package fr.igpolytech.bernads.runtime

import fr.igpolytech.bernads.cleanning.DataCleaner
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector

object BernadsDataCleaner {

  val STRING_COLUMNS = Array(
    "osClear",
    "media",
    "appOrSite",
    "city",
    "exchange",
    "publisher",
    "type",
    // "impid",
    "network"
    // "user"
  )

  val OTHERS_COLUMNS = Array(
    "interestsClear"
  )

  val NULL_POLICY = Map(
    "osClear" -> "keep",
    "media" -> "keep",
    "appOrSite" -> "keep",
    "interestsClear" -> "keep",
    "city" -> "keep",
    "exchange" -> "keep",
    "publisher" -> "keep",
    "type" -> "keep",
    // "impid" -> "keep",
    "network" -> "keep"
    // "user" -> "keep"
  )

  def cleaner(cleaner: DataCleaner): DataCleaner = cleaner
    .normalizeLabel("label", "labelInt")
    .cleanInput("os", "osClear", udf[String, String] { os =>
      if (os == null) "NULL"
      else
        os.toUpperCase match {
          case osName if osName.contains("WINDOW") => "WINDOWS"
          case osName => osName
        }
    })
    .cleanInput("interests", "interestsClear", udf[Vector, String] { interests =>
      val array = new Array[Double](26)
      if (interests != null)
        interests.split(",").filter { interest =>
          interest.startsWith("IAB")
        }.filterNot { interest =>
          interest.contains("-")
        }.map { interest =>
          interest.substring(3).toInt
        }.foreach { index =>
          array(index - 1) = 1.0
        }
      Vectors.dense(array)
    })
    .normalizeStringInputs(STRING_COLUMNS, "Indexed", NULL_POLICY)
    .compact(STRING_COLUMNS.map { _.concat("Indexed") } ++ OTHERS_COLUMNS, "features")

}
