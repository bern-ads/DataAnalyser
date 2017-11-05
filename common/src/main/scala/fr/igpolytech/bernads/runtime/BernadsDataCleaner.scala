package fr.igpolytech.bernads.runtime

import fr.igpolytech.bernads.cleanning.{DataCleaner, DataCleanerHelper}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame

class BernadsDataCleaner extends DataCleaner {

  override def clean(dataFrame: DataFrame) = {
    DataCleanerHelper(dataFrame)
      .cleanInput("os", "osClear", udf[String, String] { os =>
        if (os == null) null
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
            interest.substring(3)
          }
        Vectors.dense(array)
      })
      .cleanInput("type", "typeClear",udf[String, String] { typ =>
        if (typ == null) null
        else
          typ.toUpperCase match {
            case typName => typName
          }
      })
      .normalizeStringInputs(BernadsDataCleaner.STRING_COLUMNS, "Indexed", BernadsDataCleaner.NULL_POLICY)
      // .encodeStringInputs(STRING_COLUMNS.map { _.concat("Indexed") }, "Encoded")
      .compact(
        BernadsDataCleaner.STRING_COLUMNS.map { _.concat("Indexed") }
          ++ BernadsDataCleaner.OTHERS_COLUMNS, "features"
      )
      .dataFrame
  }

}

object BernadsDataCleaner {

  val STRING_COLUMNS = Array(
    "osClear",
    "media",
    "appOrSite",
    "city",
    "exchange",
    "publisher",
    "bidfloor",
    "typeClear",
    "network"
  )

  val OTHERS_COLUMNS: Array[String] = Array(
    "interestsClear"
  )

  val NULL_POLICY = Map(
    "osClear" -> "keep",
    "media" -> "keep",
    "appOrSite" -> "keep",
    "city" -> "keep",
    "exchange" -> "keep",
    "publisher" -> "keep",
    "bidfloor" -> "skip",
    "typeClear" -> "keep",
    "impid" -> "keep",
    "network" -> "keep"
  )

}
