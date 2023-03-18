package datacloud.hbase

import scala.io.Source
import java.io.File
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._

object LastfmFilling {

  val conf = HBaseConfiguration.create
  conf.set("hbase.zookeeper.quorum", "localhost")
  val connection = ConnectionFactory.createConnection(conf)

  def fromFile(data: File, dest: TableName, col_fam: String): Unit = {
    val colfam = Bytes.toBytes(col_fam)
    val table = connection.getTable(dest)
    for (line <- Source.fromFile(data).getLines) {
      val elems = line.split(' ')
      val rowkey = Bytes.toBytes(elems(0) + elems(1))
      val get: Get = new Get(rowkey)
      synchronized {
        val res: Result = table.get(get)
        if (res.isEmpty) {
          val put: Put = new Put(rowkey)
          put.addColumn(colfam, Bytes.toBytes("userid"), Bytes.toBytes(elems(0)))
          put.addColumn(colfam, Bytes.toBytes("trackid"), Bytes.toBytes(elems(1)))
          put.addColumn(colfam, Bytes.toBytes("locallistening"), Bytes.toBytes(elems(2).toLong))
          put.addColumn(colfam, Bytes.toBytes("radiolistening"), Bytes.toBytes(elems(3).toLong))
          put.addColumn(colfam, Bytes.toBytes("skip"), Bytes.toBytes(elems(4).toLong))
          table.put(put)
        } else {
          val map = res.getNoVersionMap
          val put: Put = new Put(rowkey)
          put.addColumn(colfam, Bytes.toBytes("locallistening"), Bytes.toBytes(elems(2).toLong + Bytes.toLong(map.get(colfam).get(Bytes.toBytes("locallistening")))))
          put.addColumn(colfam, Bytes.toBytes("radiolistening"), Bytes.toBytes(elems(3).toLong + Bytes.toLong(map.get(colfam).get(Bytes.toBytes("radiolistening")))))
          put.addColumn(colfam, Bytes.toBytes("skip"), Bytes.toBytes(elems(4).toLong + Bytes.toLong(map.get(colfam).get(Bytes.toBytes("skip")))))
          table.put(put)
        }
      }
    }
  }

}