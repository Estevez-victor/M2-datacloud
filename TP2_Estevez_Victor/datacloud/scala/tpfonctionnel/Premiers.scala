package datacloud.scala.tpfonctionnel

object Premiers {
  def premiers(n: Int): List[Int] = {
    val l = (2 to n).toList
    l.filter((x: Int) => !((2 to x - 2) exists (x % _ == 0)))
  }

  def premiersWithRec(n: Int): List[Int] = {
    if (n < 2) {
      List[Int]()
    } else {
      if (!((2 to n - 2) exists (n % _ == 0))) {
        n :: premiersWithRec(n - 1)
      } else {
        premiersWithRec(n - 1)
      }
    }
  }

}