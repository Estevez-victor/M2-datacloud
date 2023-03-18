package datacloud.synthese

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import com.datastax.spark.connector._
import java.util.ArrayList

object ImmatUtil {
  case class Proprietaire(id: String, nom: String, prenom: String, date_naissance: String, adresse: String)
  case class ModeleVehicule(id: String, marque: String, modele: String, annee_sortie: Int)
  case class Immatriculation(numero: String, date_mise_circu: Int, idmodele: String, proprios: Map[Int, String], statut: String)

  def changeAdresse(id: String, addresse: String, sc: SparkContext): Unit = {
    val rddP = sc.cassandraTable[Proprietaire]("immat", "proprietaire")
    val rdd = rddP.filter(f => f.id.equals(id)).map(x => (x.id, addresse))
    rdd.saveToCassandra("immat", "proprietaire", SomeColumns("id" as "_1", "adresse" as "_2"))
  }

  def topThree(anee: Int, sc: SparkContext): Iterable[String] = {
    val rddI = sc.cassandraTable[Immatriculation]("immat", "immatriculation")
    val rdd1 = rddI.filter(f => f.date_mise_circu.equals(anee)).map(x => (x.idmodele, 1)).reduceByKey(_ + _).map(y => (y._1, y._2))
    val rddMV = sc.cassandraTable[ModeleVehicule]("immat", "modelevehicule").map(f => (f.id, f.marque))
    val rdd2 = rddMV.join(rdd1).map(f => (f._2._1, f._2._2))
    val rdd3 = rdd2.reduceByKey(_ + _).sortBy(f => f._2, false).map(x => x._1)
    rdd3.take(3)
  }

  def fillTracingTable(ds: DStream[String], tn: TableName, c: Connection, sc: SparkContext): Unit = {
    //non fait

  }

  def getJourneyOf(immatriculation: String, tn: TableName, c: Connection): Seq[(Long, Long)] = {
    //non fait

    return null
  }
}