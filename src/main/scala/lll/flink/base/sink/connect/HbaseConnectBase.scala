package lll.flink.base.sink.connect

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.slf4j.{Logger, LoggerFactory}

class HbaseConnectBase(zkList: String, zkPort: String) extends Serializable {

    @transient
    var hbase_connection: Connection = _

    @transient
    var hbase_conf: Configuration = _

    @transient
    var table: Table = _

    //保持连接正常
    def keepAlive(): Unit = this.synchronized{

        if(hbase_conf == null){

            hbase_conf = HBaseConfiguration.create()

            hbase_conf.set("hbase.zookeeper.quorum", zkList)

            hbase_conf.set("hbase.zookeeper.property.clientPort", zkPort)

        }

        var flag: Boolean = true

        for(i <- 1 to 10 if flag){
            try {
                if(hbase_connection == null || hbase_connection.isClosed) {
                    hbase_connection = ConnectionFactory.createConnection(hbase_conf)
                    table.close()
                    table = null
                }
                else
                    flag = false
            }catch {
                case _: Exception => HbaseConnectBase.logger.warn(s"Hbase Connection is interruption, Retry Count: $i")
            }
        }

        if(flag) throw new IOException("Hbase Connection is interruption")

    }

    def getTable(tableName: String): Table ={
        this.keepAlive()
        if(table == null || !table.getName.getNameAsString.equals(tableName))
            table = hbase_connection.getTable(TableName.valueOf(tableName))
        table
    }

    def close(): Unit = hbase_connection.synchronized{
        if(hbase_connection != null && !hbase_connection.isClosed)
            hbase_connection.close()
        table.close()
    }

}

object HbaseConnectBase{

    private val logger: Logger = LoggerFactory.getLogger(HbaseConnectBase.getClass)

    def apply(zkList: String, zkPort: String): HbaseConnectBase = new HbaseConnectBase(zkList, zkPort)

}
