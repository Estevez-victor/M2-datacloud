package datacloud.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.apache.hadoop.hbase.client.TableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTable
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

class HbaseClient(val connection: Connection) {
  val admin = connection.getAdmin

  def createTable(tn: TableName, colfams: String*): Unit = {
    if (!admin.tableExists(tn)) {
      val tdb: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn)
      for (cf <- colfams) {
        tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes).build());
      }
      if (!admin.listNamespaces.contains(tn.getNamespaceAsString)) {
        admin.createNamespace(NamespaceDescriptor.create(tn.getNamespaceAsString).build())
      }
      admin.createTable(tdb.build())
    }
  }

  def deleteTable(tn: TableName): Unit = {
    val savename = tn.getNamespace
    val savenaeS = tn.getNamespaceAsString
    if (admin.tableExists(tn)) {
      admin.disableTable(tn)
      admin.deleteTable(tn)
      if (admin.listTableDescriptorsByNamespace(savename).size == 0) {
        admin.deleteNamespace(savenaeS)
      }
    }
  }

  def writeObject[E](tn: TableName, rowkey: Array[Byte], e: E)(implicit f: E => Map[(String, String), Array[Byte]]): Unit = {
    val put: Put = new Put(rowkey)
    val entries = f(e)
    for (es <- entries.keys) {
      put.addColumn(Bytes.toBytes(es._1), Bytes.toBytes(es._2), entries.get(es).get)
    }
    connection.getTable(tn).put(put)
  }

  def readObject[E](tn: TableName, rowkey: Array[Byte])(implicit f: Map[(String, String), Array[Byte]] => E): Option[E] = {
    val table = connection.getTable(tn)
    val get: Get = new Get(rowkey)
    val res: Result = table.get(get)
    var mapres = new scala.collection.immutable.HashMap[(String, String), Array[Byte]]
    for (level1 <- res.getNoVersionMap()) {
      for (level2 <- level1._2) {
        mapres = mapres + ((Bytes.toString(level1._1), Bytes.toString(level2._1)) -> level2._2)
      }
    }
    return Option(f(mapres))
  }

}


//depuis result utiliser getnoVersionMap