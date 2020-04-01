package lll.flink.base.sink.table

import org.apache.hadoop.hbase.client.Put

import scala.collection.mutable

class TableBase(row_key: String, charset: String = "UTF-8", family: String = "cf") extends Serializable {

    //[column, value]
    val col = new mutable.HashMap[String, String]()

    def getPut: Put = {
        val put = new Put(row_key.getBytes(charset))
        col.foreach(kv => put.addColumn(family.getBytes(charset), kv._1.getBytes(charset), kv._2.getBytes(charset)))
        put
    }

    def setCol(column: String, value: String): Unit = col.put(column, value)

    override def toString: String = s"row_key: $row_key \t family: $family \t charset: $charset"

}
