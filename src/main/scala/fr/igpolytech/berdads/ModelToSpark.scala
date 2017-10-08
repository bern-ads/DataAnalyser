package fr.igpolytech.berdads

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint

trait ModelToSpark {
  def label(): Double
  def features(): Vector
  def toLabeledPoint = LabeledPoint(label(), features())
}
