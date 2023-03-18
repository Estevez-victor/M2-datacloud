package datacloud.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import scala.collection.JavaConversions._
import scala.collection.mutable._

object Stereoprix {
  val conf = HBaseConfiguration.create
  conf.set("hbase.zookeeper.quorum", "localhost")
  val connection = ConnectionFactory.createConnection(conf)

  def nbVenteParCategorie(): Map[String, Int] = {
    val scan: Scan = new Scan()
    val colfam = Bytes.toBytes("defaultcf")
    scan.addColumn(colfam, Bytes.toBytes("produit"))
    val results: ResultScanner = connection.getTable(TableName.valueOf("stereoprix", "vente")).getScanner(scan)
    val mapVente = new HashMap[String, Int]
    for (res <- results) {
      for (level1 <- res.getNoVersionMap()) {
        for (level2 <- level1._2) {
          val produit = Bytes.toString(level2._2)
          if (Bytes.toString(level2._1).equals("produit")) {
            if (mapVente.contains(produit)) {
              mapVente.put(produit, mapVente.get(produit).get + 1)
            } else {
              mapVente.put(produit, 1)
            }
          }
        }
      }
    }
    val scan2 = new Scan()
    scan2.addColumn(colfam, Bytes.toBytes("idprod"))
    scan2.addColumn(colfam, Bytes.toBytes("categorie"))
    val results2: ResultScanner = connection.getTable(TableName.valueOf("stereoprix", "produit")).getScanner(scan2)
    for (res <- results2) {
      for (level1 <- res.getNoVersionMap()) {
        var produit = "none"
        var categorie = "none"
        for (level2 <- level1._2) {
          if (Bytes.toString(level2._1).equals("idprod")) {
            if (mapVente.contains(Bytes.toString(level2._2))) {
              produit = Bytes.toString(level2._2)
            }
          }
          if (Bytes.toString(level2._1).equals("categorie")) {
            categorie = Bytes.toString(level2._2)
          }
        }
        if (!(produit.equals("none"))) {
          if (mapVente.contains(categorie)) {
            mapVente.put(categorie, mapVente.get(categorie).get + mapVente.remove(produit).get)
          } else {
            mapVente.put(categorie, mapVente.remove(produit).get)
          }
        }
        produit = "none"
      }
    }
    val scan3 = new Scan()
    scan2.addColumn(colfam, Bytes.toBytes("idcat"))
    scan2.addColumn(colfam, Bytes.toBytes("designation"))
    val results3: ResultScanner = connection.getTable(TableName.valueOf("stereoprix", "categorie")).getScanner(scan3)
    for (res <- results3) {
      for (level1 <- res.getNoVersionMap) {
        var idcat = ""
        var designation = ""
        for (level2 <- level1._2) {
          if (Bytes.toString(level2._1).equals("idcat")) {
            idcat = Bytes.toString(level2._2)
          }
          if (Bytes.toString(level2._1).equals("designation")) {
            designation = Bytes.toString(level2._2)
          }
        }
        if (mapVente.contains(idcat)) {
          mapVente.put(designation, mapVente.remove(idcat).get)
        }
      }
    }
    mapVente
  }

  def denormalise(): Unit = {
    val venteTable = connection.getTable(TableName.valueOf("stereoprix", "vente"))
    val produitTable = connection.getTable(TableName.valueOf("stereoprix", "produit"))
    val catTable = connection.getTable(TableName.valueOf("stereoprix", "categorie"))
    val colfam = Bytes.toBytes("defaultcf")
    val scan = new Scan()
    scan.addColumn(colfam, Bytes.toBytes("idvente"))
    scan.addColumn(colfam, Bytes.toBytes("produit"))
    val results = venteTable.getScanner(scan)
    for (res <- results) {
      val get: Get = new Get(res.getNoVersionMap.get(colfam).get(Bytes.toBytes("produit")))
      get.addColumn(colfam, Bytes.toBytes("categorie"))
      val cat = produitTable.get(get).getNoVersionMap.get(colfam).get(Bytes.toBytes("categorie"))
      val get2= new Get(cat)
      get2.addColumn(colfam, Bytes.toBytes("designation"))
      val des=catTable.get(get2).getNoVersionMap.get(colfam).get(Bytes.toBytes("designation"))
      val put: Put = new Put(res.getNoVersionMap.get(colfam).get(Bytes.toBytes("idvente")))
      put.addColumn(Bytes.toBytes("defaultcf"), Bytes.toBytes("designation"), des)
      venteTable.put(put)
    }
  }

  def nbVenteParCategorieDenormalise(): Map[String, Int] = {
    val colfam = Bytes.toBytes("defaultcf")
    val scan = new Scan()
    scan.addColumn(colfam, Bytes.toBytes("designation"))
    val results=connection.getTable(TableName.valueOf("stereoprix","vente")).getScanner(scan)
    val mapres= new HashMap[String, Int]
    for(res <- results){
      val cat=Bytes.toString(res.getNoVersionMap.get(colfam).get(Bytes.toBytes("designation")))
      if(mapres.contains(cat)){
        mapres.put(cat, mapres.get(cat).get+1)
      }else{
        mapres.put(cat,1)
      }
    }
    mapres
  }

  def addVente(c: Connection, idvente: String, idclient: String, idmag: String, idprod: String, date: String): Unit = {
    val put: Put = new Put(Bytes.toBytes(idvente))
    val colfam = Bytes.toBytes("defaultcf")
    put.addColumn(colfam, Bytes.toBytes("idvente"), Bytes.toBytes(idvente))
    put.addColumn(colfam, Bytes.toBytes("client"), Bytes.toBytes(idclient))
    put.addColumn(colfam, Bytes.toBytes("produit"), Bytes.toBytes(idprod))
    put.addColumn(colfam, Bytes.toBytes("magasin"), Bytes.toBytes(idmag))
    put.addColumn(colfam, Bytes.toBytes("date"), Bytes.toBytes(date))
    val get= new Get(Bytes.toBytes(idprod))
    get.addColumn(colfam, Bytes.toBytes("categorie"))
    val res = c.getTable(TableName.valueOf("stereoprix", "produit")).get(get)
    val categorie = res.getNoVersionMap.get(colfam).get(Bytes.toBytes("categorie"))
    val get2= new Get(categorie)
    get.addColumn(colfam, Bytes.toBytes("designation"))
    val design = c.getTable(TableName.valueOf("stereoprix", "categorie")).get(get2).getNoVersionMap.get(colfam).get(Bytes.toBytes("designation"))
    put.addColumn(colfam, Bytes.toBytes("designation"), design)
    c.getTable(TableName.valueOf("stereoprix", "vente")).put(put)
  }

}
