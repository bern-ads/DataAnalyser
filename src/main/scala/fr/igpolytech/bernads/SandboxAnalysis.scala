package fr.igpolytech.bernads

import fr.igpolytech.bernads.runtyme.BernadsApp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.functions.desc

class SandboxAnalysis(dataPath: String) extends BernadsApp {

  override def configure(builder: Builder): Builder = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    builder.master("local[*]").config("spark.executor.memory", "15g")
  }

  override def run(): Unit = {
    val df = readJson(dataPath)

    df.where(df("appOrSite").isNull).groupBy("label", "appOrSite").count().orderBy(desc("count")).show(500, false)
    df.where(df("bidfloor").isNull).groupBy("label", "bidfloor").count().orderBy(desc("count")).show(500, false)
    df.where(df("city").isNull).groupBy("label", "city").count().orderBy(desc("count")).show(500, false)
    df.where(df("exchange").isNull).groupBy("label", "exchange").count().orderBy(desc("count")).show(500, false)
    df.where(df("impid").isNull).groupBy("label", "impid").count().orderBy(desc("count")).show(500, false)
    df.where(df("interests").isNull).groupBy("label", "interests").count().orderBy(desc("count")).show(500, false)
    df.where(df("media").isNull).groupBy("label", "media").count().orderBy(desc("count")).show(500, false)
    df.where(df("network").isNull).groupBy("label", "network").count().orderBy(desc("count")).show(500, false)
    df.where(df("os").isNull).groupBy("label", "os").count().orderBy(desc("count")).show(500, false)
    df.where(df("publisher").isNull).groupBy("label", "publisher").count().orderBy(desc("count")).show(500, false)
    df.where(df("size").isNull).groupBy("label", "size").count().orderBy(desc("count")).show(500, false)
    df.where(df("timestamp").isNull).groupBy("label", "timestamp").count().orderBy(desc("count")).show(500, false)
    df.where(df("type").isNull).groupBy("label", "type").count().orderBy(desc("count")).show(500, false)
    df.where(df("user").isNull).groupBy("label", "user").count().orderBy(desc("count")).show(500, false)
  }

}
