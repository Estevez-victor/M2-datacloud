import org.apache.spark._
import com.datastax.spark.connector._
import org.apache.spark.sql._
import Question1.Mecanicien
import Question2.Vehicule
import java.util.ArrayList
import scala.collection.JavaConversions._

object Question3 extends App {
  val conf = new SparkConf().setAppName("Spark on Cassandra 2")
    .setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().config("spark.cassandra.connection.host", "localhost").getOrCreate()
  sc.setLogLevel("OFF")
  case class MecanicienPlus(idmecano: Int, nom: String, prenom: String, status: String, vehicules: List[Int])
  val rddM = sc.cassandraTable[Mecanicien]("garage", "mecanicien")
  val rddV = sc.cassandraTable[Vehicule]("garage", "vehicule")
  val mec = new ArrayList[MecanicienPlus]()
  val ves = new ArrayList[Int]()
  for (m <- rddM.collect()) {
    for (v <- rddV.filter(_.mecano.equals(m.idmecano))) {
      ves.add(v.idvehicule)
    }
    mec.add(new MecanicienPlus(m.idmecano, m.nom, m.prenom, m.status, ves.toList))
    ves.clear()
  }
  val rdd = sc.parallelize(mec)
  spark.sql("ALTER TABLE garage.mecanicien ADD vehicules list<text>;")
  rdd.saveToCassandra("garage", "mecanicien")
  sc.stop()
  spark.stop()
}