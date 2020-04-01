package lll.flink.base.sink.logutils

import java.io.IOException

import lll.flink.base.sink.connect.HbaseConnectBase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class HbaseConnect(zkList: String, zkPort: String) extends HbaseConnectBase(zkList, zkPort) {

    private val hputs = new mutable.HashMap[String, mutable.HashMap[String, Put]]()

    def insertPutToMemory(tableName: String, family: String, rowKey: String, column: String, value: String): Unit = hputs.synchronized{
        val put_map= hputs.getOrElseUpdate(tableName, new mutable.HashMap[String, Put])
        put_map.getOrElseUpdate(rowKey, new Put(rowKey.getBytes("UTF-8")))
                .addColumn(family.getBytes(), column.getBytes(), value.getBytes())
    }

    import scala.collection.JavaConverters._
    def flush(): Unit = hputs.synchronized{
        this.keepAlive()
        hputs.keys.foreach(tableName => {
            val table: Table = hbase_connection.getTable(TableName.valueOf(tableName))
            val puts: List[Put] = hputs.getOrElse(tableName, new mutable.HashMap[String, Put]).values.toList
            table.put(puts.asJava)
            table.close()
        })
        hputs.clear()
    }

}

object HbaseConnect{

    private val logger: Logger = LoggerFactory.getLogger(HbaseConnect.getClass)

    def apply(zkList: String, zkPort: String): HbaseConnect = new HbaseConnect(zkList, zkPort)

}