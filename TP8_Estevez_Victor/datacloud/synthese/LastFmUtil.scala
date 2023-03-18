package datacloud.synthese

import org.apache.spark.rdd.RDD
import scala.io.Source
import java.io.File
import java.nio.charset.Charset
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import java.util.ArrayList

object LastFmUtil {

  def fillfromFile(files: File, tn: TableName, colfam: String, sc: SparkContext): Unit = {
    val cl = Bytes.toBytes(colfam)
    val mesSeq = new ArrayList[String]
    for (f <- files.listFiles()) {
      mesSeq.addAll(Source.fromFile(f).getLines().toSeq)
    }
    val rdd = sc.parallelize(mesSeq)
    val rdd1 = rdd.map(_.split(' ')).map(x => ((x(0), x(1)), (x(2).toLong, x(3).toLong, x(4).toLong)))
    val rdd2 = rdd1.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
    val rdd3 = rdd2.map(z => ((z._1._1 + z._1._2), (z._1._1, z._1._2, z._2._1, z._2._2, z._2._3)))
    val uid = Bytes.toBytes("userid")
    val tid = Bytes.toBytes("trackid")
    val ll = Bytes.toBytes("locallistening")
    val rl = Bytes.toBytes("radiolistening")
    val s = Bytes.toBytes("skip")
    val rddM = rdd3.map(x => new Put(Bytes.toBytes(x._1))
      .addColumn(cl, uid, Bytes.toBytes(x._2._1)).addColumn(cl, tid, Bytes.toBytes(x._2._2))
      .addColumn(cl, ll, Bytes.toBytes(x._2._3)).addColumn(cl, rl, Bytes.toBytes(x._2._4))
      .addColumn(cl, s, Bytes.toBytes(x._2._5)))
    new SparkHbaseConnector.RDDHbase(rddM).saveAsHbaseTable(tn.getNameAsString)
  }

}