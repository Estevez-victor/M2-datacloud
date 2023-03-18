package datacloud.scala.tpfonctionnel

object Statistics {
  def average(ln: List[(Double, Int)]): Double = {
    (ln.map(t => (t._1 * t._2)).reduce(_ + _)) / (ln.map(t => t._2).reduce(_ + _))
  }

}