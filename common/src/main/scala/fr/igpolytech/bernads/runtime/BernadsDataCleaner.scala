package fr.igpolytech.bernads.runtime

import fr.igpolytech.bernads.cleanning.DataCleaner
import org.apache.spark.sql.functions.udf

object BernadsDataCleaner {

  val STRING_COLUMNS = Array(
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
    .normalizeStringInputs(STRING_COLUMNS, "Indexed")
    .compact(STRING_COLUMNS.map { _.concat("Indexed") }, "features")

}
