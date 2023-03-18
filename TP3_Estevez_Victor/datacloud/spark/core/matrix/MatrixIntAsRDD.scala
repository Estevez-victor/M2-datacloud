package datacloud.spark.core.matrix

import org.apache.spark.rdd.RDD
import datacloud.scala.tpobject.vector.VectorInt
import org.apache.spark.SparkContext
import shapeless.union

object MatrixIntAsRDD {

  implicit def conversion(rdd: RDD[VectorInt]): MatrixIntAsRDD = new MatrixIntAsRDD(rdd)

  def makeFromFile(url: String, nbPar: Int, spark: SparkContext): MatrixIntAsRDD = {
    val rdd1 = spark.textFile(url)
    val rdd2 = rdd1.map(_.split(' '))
    val rdd3 = rdd2.map(x => new VectorInt(x.map(_.toInt)))
    val rdd4 = rdd3.zipWithIndex
    val rdd5 = rdd4.sortBy(x => x._2, true, nbPar)
    val rdd6 = rdd5.map(x => x._1)
    rdd6
  }

}

class MatrixIntAsRDD(val lines: RDD[VectorInt]) {

  override def toString = {
    val sb = new StringBuilder()
    lines.collect().foreach(line => sb.append(line + "\n"))
    sb.toString()
  }

  def nbLines(): Int = lines.count().toInt

  def nbColumns(): Int = lines.first().length()

  def get(i: Int, j: Int): Int = lines.zipWithIndex().filter(_._2 == i).first()._1.elements(j)

  override def equals(a: Any): Boolean = a match {
    case a: MatrixIntAsRDD =>
      var res = false
      if ((a.nbLines().equals(this.nbLines())) && (a.nbColumns().equals(this.nbColumns()))) {
        var nbpar = 1
        if (lines.partitions.size <= a.lines.partitions.size) {
          nbpar = lines.partitions.size
        } else {
          nbpar = a.lines.partitions.size
        }
        val rdd = (lines.zipWithIndex().map(x => (x._2, x._1)).sortByKey(true, nbpar)).zip((a.lines.zipWithIndex().map(x => (x._2, x._1)).sortByKey(true, nbpar)))
        val rdd2 = rdd.map(x => x._1._2.equals(x._2._2))
        res = rdd2.reduce(_ && _)
      }
      res
    case _ => false
  }

  def +(other: MatrixIntAsRDD): MatrixIntAsRDD = {
    val rdd = lines.zip(other.lines)
    val rddAdd = rdd.map(x => x._1 + x._2)
    rddAdd
  }

  def transpose(): MatrixIntAsRDD = {
    val rdd = lines.zipWithIndex
    val rdd2 = rdd.flatMap {
      case (row, rowIndex) => row.asInstanceOf[VectorInt].elements.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, number)
      }
    }
    val rdd3 = rdd2.groupByKey.sortByKey().values
    val rdd4 = rdd3.map(x => x.toArray.sortBy(_._1).map(_._2))
    val rdd5 = rdd4.map(x => new VectorInt(x))
    rdd5
  }

  def *(other: MatrixIntAsRDD): MatrixIntAsRDD = {
    val rdd = this.transpose().lines.zipWithIndex()
    val rdd2 = rdd.union(other.lines.zipWithIndex())
    val rdd3 = rdd2.map(x => (x._2, x._1))
    val rdd4 = rdd3.groupByKey()
    val rdd5 = rdd4.map(x => (x._1, x._2.head.prodD(x._2.last))).values
    val rdd6 = rdd5.flatMap(x => x.zipWithIndex)
    val rdd7 = rdd6.map(x => (x._2, x._1))
    val rdd8 = rdd7.groupByKey()
    val rdd9 = rdd8.map(x => x._2.reduce(_ + _))
    new MatrixIntAsRDD(rdd9)
  }
}

