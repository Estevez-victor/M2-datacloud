package datacloud.scala.tpfonctionnel
import scala.math.Ordering._

object MySorting {
  def isSorted[A](a: Array[A], f: (A, A) => Boolean): Boolean = {
    for (i <- 0 to a.length - 1) {
      if (i + 1 < a.length) {
        if (!(f(a(i), a(i + 1)))) {
          return false
        }
      }
    }
    true
  }

  def ascending[T](a: T, b: T)(implicit o: Ordering[T]): Boolean = {
    val x: Int = o.compare(a, b)
    if (x <= 0) {
      true
    } else {
      false
    }
  }
  def descending[T](a: T, b: T)(implicit o: Ordering[T]): Boolean={
    val x: Int = o.compare(a, b)
    if (x <= 0) {
      false
    } else {
      true
    }
  }
}