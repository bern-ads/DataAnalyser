package fr.igpolytech.bernads.model

import fr.igpolytech.bernads.runtime.BernadsApp

object Main {

  val DEFAULT_MODEL_FILE_NAME = "bernads.spark.model"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("The model takes at least one parameter: the path to the labeled json data file.")
    }

    val dataPath = args(0)
    val modelPath = if (args.length == 2)
      args(1)
    else
      DEFAULT_MODEL_FILE_NAME

    BernadsApp(new BernadsModel(dataPath, modelPath))
  }

}
