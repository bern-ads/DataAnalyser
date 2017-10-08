package fr.igpolytech.berdads
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

case class PersonModel(
                        os: String,
                        appOrSite: String,
                        media: String,
                        clicked: Boolean
                      ) extends ModelToSpark {
  override def label(): Double = if (clicked) 1.0 else 0.0

  override def features(): Vector = PersonModel.convert(this)
}

object PersonModel {
  def apply(row: Row): PersonModel = new PersonModel(
    row.getAs("os"),
    row.getAs("appOrSite"),
    row.getAs("media"),
    row.getAs("label")
  )

  def categoricalFeaturesInfo() = Map(
    0 -> 4,
    1 -> 2,
    2 -> 2
  )

  def convert(personModel: PersonModel): Vector = Vectors.dense(
    personModel.os match {
      case "ANDROID" => 0d
      case "IOS" => 1d
      case "WINDOWS" => 2d
      case _ => 3d
    },
    personModel.appOrSite match {
      case "app" => 0d
      case "site" => 1d
    },
    personModel.media match {
      case "d476955e1ffb87c18490e87b235e48e7" => 0d
      case "343bc308e60156fb39cd2af57337a958" => 1d
    }
  )
}
