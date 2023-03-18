package datacloud.synthese

import org.apache.hadoop.hbase.TableName
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Scan
import scala.collection.JavaConversions._
import java.util.ArrayList
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor

object HbaseSparkUtil {
  def copyTable(src: TableName, dest: TableName, sc: SparkContext, c: Connection): Unit = {
    val rdd = new SparkHbaseConnector.MySparkContext(sc).hbaseTableRDD(src.getNameAsString, new Scan)
    val rdd2 = rdd.map(x => {
      val put: Put = new Put(x._1.get)
      for (level1 <- x._2.getNoVersionMap) {
        for (level2 <- level1._2) {
          put.addColumn(level1._1, level2._1, level2._2)
        }
      }
      put
    })
    if (c.getAdmin.tableExists(dest)) {
      c.getAdmin.disableTable(dest)
      c.getAdmin.deleteTable(dest)
    }
    val oneRow = rdd.map(x => x._1.get).first()
    val Get = new Get(oneRow)
    val table = c.getTable(src)
    val oneRes = table.get(Get)
    val tbd = TableDescriptorBuilder.newBuilder(dest)
    val cfd = new ArrayList[ColumnFamilyDescriptor]
    for (level1 <- oneRes.getNoVersionMap) {
      cfd.add(ColumnFamilyDescriptorBuilder.newBuilder(level1._1).build())
    }
    table.close()
    tbd.setColumnFamilies(cfd)
    c.getAdmin.createTable(tbd.build())
    new SparkHbaseConnector.RDDHbase(rdd2).saveAsHbaseTable(dest.getNameAsString)
  }
}