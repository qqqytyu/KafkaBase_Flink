package lll.flink.base.sink

import lll.flink.base.sink.connect.HbaseConnectBase
import lll.flink.base.sink.context.{MyContextSer, MyHbaseSinkContext}
import lll.flink.base.sink.table.TableBase
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.hadoop.hbase.client.{Put, Table}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

class MyHbaseSinkFunction(zkList: String, zkPort: String, tableName: String)
        extends TwoPhaseCommitSinkFunction[TableBase, MyHbaseSinkFunction.MyHbaseTransaction, MyHbaseSinkContext](
            new KryoSerializer[MyHbaseSinkFunction.MyHbaseTransaction](classOf[MyHbaseSinkFunction.MyHbaseTransaction], new ExecutionConfig), new MyContextSer) {

    private val connect: HbaseConnectBase = HbaseConnectBase(zkList, zkPort)

    //第一步：开启一个事务，准备将数据写入到临时内存中
    override def beginTransaction(): MyHbaseSinkFunction.MyHbaseTransaction = new MyHbaseSinkFunction.MyHbaseTransaction

    //第二步：将流入的数据，实时写入到临时内存中
    override def invoke(txn: MyHbaseSinkFunction.MyHbaseTransaction, in: TableBase, context: SinkFunction.Context[_]): Unit = if(in.col.nonEmpty) txn.send(in)

    //第三步：预提交，将临时内存中的数据转化为Put数据，准备提交到Hbase，并开启一个新的事务 - beginTransaction
    override def preCommit(txn: MyHbaseSinkFunction.MyHbaseTransaction): Unit = txn.flush()

    //第四步：提交数据，将转化后的Put数据提交到Hbase中
    override def commit(txn: MyHbaseSinkFunction.MyHbaseTransaction): Unit = txn.put(connect.getTable(tableName))

    //异常处理：两阶段提交的步骤中，如果出现异常，则会跳转到此步骤
    override def abort(txn: MyHbaseSinkFunction.MyHbaseTransaction): Unit = {
        MyHbaseSinkFunction.logger.error("数据写入Hbase失败...")
    }

}

object MyHbaseSinkFunction{

    private val logger: Logger = LoggerFactory.getLogger(MyHbaseSinkFunction.getClass)

    /**
      * 自定义Flink sink 事务实现类
      * 向Hbase写入数据
      */
    class MyHbaseTransaction extends Serializable {

        //调用invoke方法时，数据存储的位置
        private var table_datas: ListBuffer[TableBase] = new ListBuffer[TableBase]()

        //调用 方法时，数据的存储位置
        private var puts: ListBuffer[Put] = _

        //将数据存储在临时内存中
        def send(tableBase: TableBase): Unit = table_datas.append(tableBase)

        //将数据转换为Put，并关闭table_datas，确保不会有新数据流入
        def flush(): Unit = {
            puts = table_datas.map(_.getPut)
            table_datas.clear()
            table_datas = null
        }

        import scala.collection.JavaConverters._
        //将内存数据一次性写入到Hbase 的指定表中
        def put(table: Table): Unit = puts match {
            case ps: ListBuffer[Put] if ps.nonEmpty => table.put(ps.asJava)
            case _ => logger.info("No columns to insert")
        }

        //清空内存数据
        def clear(): Unit = puts.clear()

    }

}
