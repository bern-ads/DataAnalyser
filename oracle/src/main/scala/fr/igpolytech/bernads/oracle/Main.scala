package fr.igpolytech.bernads.oracle

import fr.igpolytech.bernads.runtime.BernadsApp

object Main {

  val DEFAULT_RESULT_FILE_NAME = "bernads.result.csv"

  def main(args: Array[String]): Unit = {
    if (args.length <= 1) {
      throw new IllegalArgumentException("The model takes at least three parameters: the path to the unlabeled json data file, the path to the selector and the path to the model.")
    }

    val dataPath = args(0)
    val selectorPath = args(1)
    val modelPath = args(2)
    val resultPath = if (args.length == 4)
      args(3)
    else
      DEFAULT_RESULT_FILE_NAME

    BernadsApp(new BernadsOracle(dataPath, selectorPath, modelPath, resultPath))
  }

}