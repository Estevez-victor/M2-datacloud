package datacloud.scala.tpfonctionnel

object FunctionParty {
  def curryfie[A, B, C](f: (A, B) => C): A => B => C = {
    (a: A) => ((b: B) => f(a, b))
  }

  def decurryfie[A, B, C](f: A => B => C): (A, B) => C = {
    (a: A, b: B) => f(a)(b)
  }

  def compose[A, B, C](f: B => C, g: A => B): A => C = {
    (a: A) => f(g(a))
  }

  def axplusb(a: Int, b: Int): Int => Int = {
    val mult = curryfie[Int, Int, Int]((a, b) => a * b)
    val add = curryfie[Int, Int, Int]((a, b) => a + b)
    (x: Int) => compose[Int, Int, Int](add(b), mult(a))(x)
  }

  /*decurryfie[A,B,C](f: A => B => C):(A, B) => C
  compose[A,B,C](f: B => C, g: A => B): A => C
  axplusb(a:Int,b:Int):Int => Int*/
}