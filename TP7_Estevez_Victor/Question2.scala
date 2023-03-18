import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import Question1.Mecanicien
import java.util.ArrayList
import scala.collection.JavaConversions._

object Question2 extends App {
  val conf = new SparkConf().setAppName("Spark on Cassandra 2")
    .setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  case class Vehicule(idvehicule: Int, kilometrage: Int, marque: String, mecano: Int, modele: String)
  case class Reparation(idvehicule: Int, kilometrage: Int, marque: String, modele: String, mecano: Int, nommecano: String, prenommecano: String, statusmecano: String)
  val rddmecano = sc.cassandraTable[Mecanicien]("garage", "mecanicien")
  val rddvehicule = sc.cassandraTable[Vehicule]("garage", "vehicule")
  val reps = new ArrayList[Reparation]()
  for (v <- rddvehicule.collect()) {
    for (m <- rddmecano.filter(_.idmecano.equals(v.mecano)).collect()) {
      reps.add(new Reparation(v.idvehicule, v.kilometrage, v.marque, v.modele, m.idmecano, m.nom, m.prenom, m.status))
    }
  }
  val rdd = sc.parallelize(reps)
  rdd.saveAsCassandraTable("garage", "reparation")
  sc.stop()
}