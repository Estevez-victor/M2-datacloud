package datacloud.scala.tpobject.bintree

object BinTrees {

  def contains(it: IntTree, i: Int): Boolean = it match {
    case it: NodeInt => if (it.elem.equals(i)) {
      true
    } else {
      contains(it.left, i) || contains(it.right, i)
    }
    case _ => false
  }

  def contains[A](at: Tree[A], x: A): Boolean = at match {
    case at: Node[A] => if (at.elem.equals(x)) {
      true
    } else {
      contains(at.left, x) || contains(at.right, x)
    }
    case _ => false
  }

  def size(it: IntTree): Int = it match {
    case it: NodeInt => 1 + size(it.left) + size(it.right)
    case _ => 0
  }
  def size[A](at: Tree[A]): Int = at match {
    case it: Node[A] => 1 + size(it.left) + size(it.right)
    case _ => 0
  }

  def copy(it: IntTree): IntTree = it match {
    case it: NodeInt => new NodeInt(it.elem, copy(it.left), copy(it.right))
    case _ => it
  }
  def insert(it: IntTree, i: Int): IntTree = {
    it match {
      case it: NodeInt => if (size(it.left) >= size(it.right)) {
        new NodeInt(it.elem, copy(it.left), insert(it.right, i))
      } else {
        new NodeInt(it.elem, insert(it.left, i), copy(it.right))
      }
      case _ => new NodeInt(i, EmptyIntTree, EmptyIntTree)
    }
  }

  def copy[A](at: Tree[A]): Tree[A] = at match {
    case at:Node[A] => new Node[A](at.elem, copy(at.left), copy(at.right))
    case _ => at
  }

  def insert[A](at: Tree[A], x: A): Tree[A] = {
    at match {
      case at: Node[A] => if (size(at.left) >= size(at.right)) {
        new Node[A](at.elem, copy(at.left), insert(at.right, x))
      } else {
        new Node[A](at.elem, insert(at.left, x), copy(at.right))
      }
      case _ => new Node[A](x, EmptyTree, EmptyTree)
    }
  }

}