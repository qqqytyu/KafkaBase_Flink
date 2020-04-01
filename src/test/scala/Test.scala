import lll.flink.base.sink.logutils.HbaseConnect
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, Connection}

import scala.collection.mutable.ListBuffer

object Test {

    def main(args: Array[String]): Unit = {

        val lbs = new ListBuffer[(String, String)]()

        for(i <- 1 to 10) {
            println(i)
            lbs.append((s"$i - lll", s"lll - $i"))
        }

        val map1: Map[String, String] = lbs.toMap

        println("map1: " + System.identityHashCode(map1))

        val map2: Map[String, String] = map1.toList.toMap

        println("map2: " + System.identityHashCode(lbs.toMap))

    }

    def hbaseTest(): Unit ={

        val connect: HbaseConnect = HbaseConnect("lll-node-02,lll-node-03,lll-node-04", "2181")

        connect.keepAlive()

        val hbase_connection: Connection = connect.hbase_connection

        val admin: Admin = hbase_connection.getAdmin

        val names: Array[TableName] = admin.listTableNames()

        names.foreach(println(_))

        admin.close()

        connect.close()

    }

}
