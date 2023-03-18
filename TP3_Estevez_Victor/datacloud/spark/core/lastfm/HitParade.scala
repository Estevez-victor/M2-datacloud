package datacloud.spark.core.lastfm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HitParade {
  case class TrackId(id: String)
  case class UserId(id: String)
  def loadAndMergeDuplicates(spark: SparkContext, in: String): RDD[((UserId, TrackId), (Int, Int, Int))] = {
    val rdd = spark.textFile(in);
    val rdd1 = rdd.map(_.split(' '))
    val rdd2 = rdd1.map(x => ((x(0), x(1)), (x(2).toInt, x(3).toInt, x(4).toInt)))
    val rdd3 = rdd2.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    val rdd4 = rdd3.map(x => ((new UserId(x._1._1), new TrackId(x._1._2)), x._2))
    rdd4
  }
  def hitparade(rdd: RDD[((UserId, TrackId), (Int, Int, Int))]): RDD[TrackId] = {
    val rdd1 = rdd.map(x => ((x._1._2), (1, x._2)))
    val rdd2 = rdd1.reduceByKey((a, b) => (a._1 + b._1, (a._2._1 + b._2._1, a._2._2 + b._2._2, a._2._3 + b._2._3)))
    val rdd3 = rdd2.map(x => (x._1, x._2._1, (x._2._2._1 + x._2._2._2 - x._2._2._3)))
    val rdd4 = rdd3.sortBy(x => (x._1.id), true).sortBy(x => (x._2, x._3), false)
    val rdd5 = rdd4.map(x => x._1)
    rdd5
  }
}