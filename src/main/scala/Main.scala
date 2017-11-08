import fr.igpolytech.bernads._
import fr.igpolytech.bernads.runtyme.BernadsApp

object Main {

  def main(args: Array[String]): Unit = {
    BernadsApp(new SandboxAnalysis("/home/yves/Téléchargements/data-students.json"))
  }

}
