package org.apache.flink.streaming.connectors.kafka

import java.util.{Properties, UUID}

import lll.flink.base.sink.logutils.HbaseConnect
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * 继承修改FlinkKafkaProducer类，在进行数据预提交和提交时，插入提交日志
 * @param topic topic
 * @param serializationSchema 序列化类
 * @param producerConfig kafka配置
 * @param semantic 提交类型
 */
class MyFlinkKafkaProducer(
    topic: String,
    serializationSchema: KafkaSerializationSchema[(String, String)],
    producerConfig: Properties,
    semantic: FlinkKafkaProducer.Semantic,
    zkList: String, zkPort: String) extends FlinkKafkaProducer[(String, String)](topic, serializationSchema, producerConfig, semantic){

    private val hbase_connect: HbaseConnect = HbaseConnect(zkList, zkPort)

    //写出数据
    override def invoke(transaction: FlinkKafkaProducer.KafkaTransactionState, next: (String, String), context: SinkFunction.Context[_]): Unit = {
        this.invokeLog(next)
        super.invoke(transaction, next, context)
    }

    //预提交
    override def preCommit(transaction: FlinkKafkaProducer.KafkaTransactionState): Unit = {
        this.preCommitLog()
        println("transactionalId: " + transaction.transactionalId)
        super.preCommit(transaction)
    }

    //将数据日期写到内存
    def invokeLog(log_data: (String, String)): Unit ={
        hbase_connect.insertPutToMemory("lll:kafka_log","cf",log_data._1,"data",log_data._2)
    }

    //预提交数据时，将要提交的内容写到日期系统中
    def preCommitLog(): Unit ={
        hbase_connect.flush()
    }



}

object MyFlinkKafkaProducer{

    private val LOG = LoggerFactory.getLogger(MyFlinkKafkaProducer.getClass)

}
