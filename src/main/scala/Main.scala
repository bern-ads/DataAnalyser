
import fr.igpolytech.bernads.Sandbox
import fr.igpolytech.bernads.runtime.BernadsApp

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("Main method take one parameter: the path to the data file !")
    }

    BernadsApp(new Sandbox, args)
  }

}
