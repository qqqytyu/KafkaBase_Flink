package lll.flink.base.kafka

import java.util.{Properties, UUID}

import lll.flink.base.sink.MyHbaseSinkFunction
import lll.flink.base.sink.table.TableBase
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.mutable

object ReadKafkaToHbase {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(2)

        env.enableCheckpointing(1000 * 10)

        val kafka_property: Properties = new Properties()
        kafka_property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "lll-node-02:9092,lll-node-03:9092,lll-node-04:9092")
        kafka_property.setProperty("group.id", "kafkaToHbase")
        kafka_property.setProperty("auto.offset.reset", "earliest")
        kafka_property.setProperty("enable.auto.commit", "false")
        kafka_property.setProperty(ProducerConfig.RETRIES_CONFIG, "5")
        kafka_property.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, s"${1000 * 60}")
        kafka_property.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")

        val consumer = new FlinkKafkaConsumer[String]("hbase-kafka2", new SimpleStringSchema(), kafka_property)
        consumer.setStartFromGroupOffsets()
        consumer.setCommitOffsetsOnCheckpoints(true)

        import org.apache.flink.streaming.api.scala._
        val kafka_source: DataStream[String] = env.addSource(consumer)

        val trunsfrom: DataStream[TableBase] = kafka_source
                .flatMap(value => {
                    value match {
                        case value: String if value != null => value.split(" ")
                        case _ => Nil
                    }
                }).map((_, 1))
                .keyBy(0)
                .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
                .map(tp => {
                    val table: TableBase = new TableBase(UUID.randomUUID().toString)
                    table.setCol("workCountName", tp._1)
                    table.setCol("workCountNum", tp._2.toString)
                    table
                })
                .map(tb => {
                    val col: mutable.HashMap[String, String] = tb.col
                    tb
                })

        val hbase_sink = new MyHbaseSinkFunction("lll-node-02,lll-node-03,lll-node-04", "2181", "lll:workCountTable")

        trunsfrom.addSink(hbase_sink)

        env.execute("kafkaToHbase")

    }

}
