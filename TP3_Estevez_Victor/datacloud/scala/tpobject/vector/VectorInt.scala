package datacloud.scala.tpobject.vector

class VectorInt(val elements: Array[Int]) extends Serializable {
  def length(): Int = elements.length
  def get(i: Int): Int = elements(i)
  override def toString(): String = {
    val sb: StringBuilder = new StringBuilder("(")
    for (i <- 0 to elements.length - 1) {
      if (i.equals(elements.length - 1)) {
        sb ++= elements(i).toString()
      } else {
        sb ++= elements(i).toString()
        sb += ' '
      }
    }
    sb += ')'
    sb.toString()
  }
  override def equals(a: Any): Boolean = a match {
    case a: VectorInt =>
      if (a.length().equals(length())) {
        for (i <- (0 to (length() - 1))) {
          if (!(a.get(i).equals(get(i)))) {
            return false
          }
        }
        return true
      }
      false
    case _ => false
  }
  def +(other: VectorInt): VectorInt = {
    val vi: VectorInt = new VectorInt(new Array[Int](length()))
    for (x <- 0 to length() - 1) {
      vi.elements(x) = elements(x) + other.elements(x)
    }
    vi
  }

  def *(v: Int): VectorInt = {
    val vi: VectorInt = new VectorInt(new Array[Int](length()))
    for (i <- 0 to length() - 1) {
      vi.elements(i) = elements(i) * v
    }
    vi
  }
  def prodD(other: VectorInt): Array[VectorInt] = {
    val avi: Array[VectorInt] = new Array[VectorInt](length())
    for (i <- 0 to length() - 1) {
      avi(i) = new VectorInt(new Array[Int](other.length()))
      for (j <- 0 to other.length() - 1) {
        avi(i).elements(j) = elements(i) * other.elements(j)
      }
    }
    avi
  }
}

object VectorInt {
  implicit def arrayIntToVectorInt(a: Array[Int]): VectorInt = new VectorInt(a)
}