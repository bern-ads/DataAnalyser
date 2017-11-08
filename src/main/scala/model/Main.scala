package model

import fr.igpolytech.bernads.runtyme.BernadsApp

object Main {

  val DEFAULT_SELECTOR_FILE_NAME = "bernads.spark.selector"
  val DEFAULT_MODEL_FILE_NAME = "bernads.spark.model"

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("The model takes at least one parameter: the path to the labeled json data file.")
    }

    val dataPath = args(0)
    val selectorPath = if (args.length == 2)
      args(1)
    else
      DEFAULT_SELECTOR_FILE_NAME
    val modelPath = if (args.length == 3)
      args(2)
    else
      DEFAULT_MODEL_FILE_NAME

    BernadsApp(new BernadsModel(dataPath, selectorPath, modelPath))
  }

}
