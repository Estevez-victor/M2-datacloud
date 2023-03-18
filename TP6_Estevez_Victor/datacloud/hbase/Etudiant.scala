package datacloud.hbase

import org.apache.hadoop.hbase.util.Bytes

case class Etudiant(val nom: String, val prenom: String, val age: Int, val notes: Map[String, Int]) {}

object Etudiant {
  def toHbaseObject(e: Etudiant): Map[(String, String), Array[Byte]] = {
    var mapres = new scala.collection.immutable.HashMap[(String, String), Array[Byte]]

    mapres = mapres.+(("info", "nom") -> Bytes.toBytes(e.nom))
    mapres = mapres.+(("info", "prenom") -> Bytes.toBytes(e.prenom))
    mapres = mapres.+(("info", "age") -> Bytes.toBytes(e.age))
    for (mat <- e.notes.keys) {
      mapres = mapres.+(("notes", mat) -> Bytes.toBytes(e.notes.get(mat).get))
    }
    mapres
  }

  def HbaseObjectToEtudiant(m: Map[(String, String), Array[Byte]]): Etudiant = {
    val nom: String = Bytes.toString(m.get(("info", "nom")).get)
    val prenom: String = Bytes.toString(m.get(("info", "prenom")).get)
    val age: Int = Bytes.toInt(m.get(("info", "age")).get)
    var notes = scala.collection.immutable.Map[String, Int]()
    for (k <- m.keySet) {
      if (k._1.equals("notes")) {
        notes = notes.+(k._2 -> Bytes.toInt(m.get((k._1, k._2)).get))
      }
    }
    Etudiant(nom, prenom, age, notes)
  }

}