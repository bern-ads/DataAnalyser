package fr.igpolytech.bernads

import fr.igpolytech.bernads.runtime.{BernadsApp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession.Builder
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Sandbox3 extends BernadsApp {

  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[*]").config("spark.executor.memory", "15g")
  }

  override def run(args: Array[String]): Unit = {
    val df = readJson(args(0))

    df.groupBy("label", "appOrSite").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "bidfloor").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "city").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "exchange").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "impid").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "interests").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "media").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "network").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "os").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "publisher").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "size").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "timestamp").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "type").count().orderBy(desc("count")).show(500, false)
    df.groupBy("label", "user").count().orderBy(desc("count")).show(500, false)
  }

}
