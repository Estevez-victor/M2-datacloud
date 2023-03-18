package datacloud.spark.core.stereoprix

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Stats {
  def chiffreAffaire(path: String, year: Int): Int = {
    val spark = new SparkContext(new SparkConf().setAppName("statsYear").setMaster("local[*]"))
    val rdd1 = spark.textFile(path);
    val rdd2 = rdd1.map(_.split(" "));
    val rdd3 = rdd2.map(x => (x(0), x(2)));
    val rdd4 = rdd3.map(x => (x._1.split("_")(2), x._2));
    val rdd5 = rdd4.filter(_._1.equals(year.toString));
    val rdd6 = rdd5.map(_._2.toInt);
    val rdd7 = rdd6.reduce(_ + _);
    spark.stop
    rdd7
  }
  def chiffreAffaireParCategorie(in: String, out: String) {
    val spark = new SparkContext(new SparkConf().setAppName("statsParCategorie").setMaster("local[*]"))
    val rdd = spark.textFile(in)
    val rdd1 = rdd.map(_.split(" "))
    val rdd2 = rdd1.map(x => (x(4), x(2).toInt))
    val rdd3 = rdd2.reduceByKey(_ + _)
    val rdd4 = rdd3.map(x => (x._1 + ':' + x._2.toString))
    rdd4.saveAsTextFile(out)
    spark.stop
  }
  def produitLePlusVenduParCategorie(in: String, out: String) {
    val spark = new SparkContext(new SparkConf().setAppName("statsVenduCategorie").setMaster("local[*]"))
    val rdd = spark.textFile(in)
    val rdd1 = rdd.map(_.split(" "))
    val rdd2 = rdd1.map(x => (x(4), x(3)))
    val rdd3 = rdd2.groupByKey()
    val rdd4 = rdd3.map(x => (x._1 + ':' + x._2.groupBy(identity).maxBy(_._2.size)._1))
    rdd4.saveAsTextFile(out)
    spark.stop
  }
}