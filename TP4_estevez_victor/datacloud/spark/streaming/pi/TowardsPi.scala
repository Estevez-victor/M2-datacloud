package datacloud.spark.streaming.pi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes

object TowardsPi extends App {
  val sc = new SparkContext(new SparkConf().setAppName("pi.TowardsPi").setMaster("local[*]"))
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(4))
  ssc.checkpoint("/tmp")
  val pi = ssc.socketTextStream("localhost", 4242)
  val pi2 = pi.map(_.split(' ')).map(x => (x(0).toDouble, x(1).toDouble))
  val nbIn = pi2.count().map(x => (1, x.toDouble))
  val pi3 = pi2.filter(x => (((x._1 * x._1) + (x._2 * x._2)) <= 1))
  val nbTotal = pi3.count().map(x => (2, x.toDouble))

  val calc = nbTotal.union(nbIn).map(x => x._2).reduce((a, b) => if (a < b) { 4.0 * a / b } else { 4.0 * b / a }).map(x => (0, x)).updateStateByKey((vals: Seq[Double], s: Option[Double]) => {
    var olds : Double = vals.head
    if(olds.isNaN()){
      olds=3
    }
    var news: Double = 0
    var ok:Double=0
    if (!s.isEmpty) {
      news = s.head
      ok = (olds + news)/2
    }else{
      ok=olds
    }
    Some(ok)
  })

  calc.map(_._2)print()
  ssc.start()
  ssc.awaitTermination()
}