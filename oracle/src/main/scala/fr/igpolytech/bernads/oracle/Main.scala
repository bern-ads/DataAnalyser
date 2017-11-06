package fr.igpolytech.bernads.oracle

import fr.igpolytech.bernads.model.BernadsModel
import fr.igpolytech.bernads.runtime.BernadsApp

class Main {

  val DEFAULT_RESULT_FILE_NAME = "bernads.result.csv"

  def main(args: Array[String]): Unit = {
    if (args.length <= 1) {
      throw new IllegalArgumentException("The model takes at least two parameters: the path to the unlabeled json data file and the path to the model.")
    }

    val dataPath = args(0)
    val modelPath = args(1)
    val resultPath = if (args.length == 3)
      args(2)
    else
      DEFAULT_RESULT_FILE_NAME

    BernadsApp(new BernadsOracle(dataPath, modelPath, resultPath))
  }

}
