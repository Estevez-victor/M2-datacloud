import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql._
import com.datastax.spark.connector.SomeColumns

object Question1 extends App {
  val conf = new SparkConf().setAppName("Spark on Cassandra 1")
  .setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  case class Mecanicien(idmecano: Int, nom: String, prenom: String, status: String)
  val rdd = sc.cassandraTable[Mecanicien]("garage", "mecanicien")
  rdd.saveAsCassandraTable("garage", "mecanicien_cpy")
  sc.stop()
}