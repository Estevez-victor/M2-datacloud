package datacloud.scala.tpfonctionnel

object Counters {
  def nbLetters(lc: List[String]): Int = {
    lc.flatMap(x => x.split(" ")).map(x => x.size).reduce((x, y) => x + y)
  }
}