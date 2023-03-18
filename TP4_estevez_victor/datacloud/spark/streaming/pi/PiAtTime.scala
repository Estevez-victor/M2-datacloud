package datacloud.spark.streaming.pi

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes

object PiAtTime extends App{
    val sc = new SparkContext(new SparkConf().setAppName("datacloud.spark.streaming.pi.PiAtTime").setMaster("local[*]"))
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(4))
    val pi = ssc.socketTextStream("localhost", 4242)
    val pi2 = pi.map(_.split(' ')).map(x => (x(0).toDouble, x(1).toDouble))
    val nbIn = pi2.count().map(_.toDouble)
    val pi3 = pi2.filter(x =>(  ((x._1*x._1)+(x._2*x._2)) <=1) )
    val nbTotal = pi3.count().map(_.toDouble)
    val calc= nbTotal.union(nbIn).reduce((a,b)=> if(a<b){4.0*a/b}else{4.0*b/a} )
    calc.print()
    ssc.start()
    ssc.awaitTermination()
}