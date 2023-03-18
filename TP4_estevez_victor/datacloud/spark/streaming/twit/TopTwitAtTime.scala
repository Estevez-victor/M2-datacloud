package datacloud.spark.streaming.twit

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes

object TopTwitAtTime extends App{
  val sc = new SparkContext(new SparkConf().setAppName("twit.TopTwitAtTime").setMaster("local[*]"))
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("/home/vicman/DATACLOUD/Twit")
  val twit= ssc.socketTextStream("localhost", 4242)
  val twit2=twit.flatMap(_.split(' '))
  val twit3= twit2.filter(x=>x(0).equals('#'))
  val twit4=twit3.map(x=>(x,1))
  val twit5=twit4.reduceByKey(_+_).map(x=>(x._2,x._1))
  val twit6=twit5.transform(myrdd=>myrdd.sortByKey(false))
  twit6.print(10)
  ssc.start()
  ssc.awaitTermination()
}