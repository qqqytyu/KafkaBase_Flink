package lll.flink.base.kafka

import java.util.Properties

import lll.flink.base.kafka.deserialize.MyDeserializationSchema
import lll.flink.base.kafka.serialize.MySerializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, MyFlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig

object MyFlinkToKafka {

    def main(args: Array[String]): Unit = {

        //初始化Flink流处理程序 自动判断是local还是集群环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //从整体上，设置flink并行度
        env.setParallelism(1)

        //开启Checkpoint，并设置多长时间设置一个检查点
        env.enableCheckpointing(1000 * 10)

        //配置kakfa运行参数
        val kafka_property: Properties = new Properties()
        //配置Kafka broker地址
        kafka_property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "lll-node-02:9092,lll-node-03:9092,lll-node-04:9092")
        //配置消费者组
        kafka_property.setProperty("group.id", "lll-group")
        //Consumer配置, 读取kafka数据时，默认的读取规则
        kafka_property.setProperty("auto.offset.reset", "earliest")
        //Consumer配置, 是否自动提交 offset
        kafka_property.setProperty("enable.auto.commit", "false")
        //可以在Kafka的Partition发生leader切换时，Flink不重启，而是做5次尝试
        kafka_property.setProperty(ProducerConfig.RETRIES_CONFIG, "5")
        //Producer配置，设置事务超时时间，这个配置值将与InitPidRequest一起发送到事务协调器。
        //如果该值大于transaction.max.timeout.ms(default: 900000)。在broke中设置ms时，请求将失败，并出现InvalidTransactionTimeout错误
        kafka_property.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, s"${1000 * 60}")
        //Producer配置，该参数指定了生产者在收到服务器响应之前可以发送多少个消息，它的值越高，就会占用越多的内存，不过也会提升吞吐量
        //把它设为 1 可以保证消息是按照发送的顺序写入服务器的，即使发生了重试（不唯一，不保证分区有序, retries 大于1时）
        kafka_property.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
        //Producer配置，请求的最大字节数, 此项设置将会限制producer每次批量发送请求的数目，以防发出巨量的请求
        //kafka_property.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, s"${1048576 * 5}")


        //创建flink版本的kafka数据源
        //参数1：topic，参数2：value的反序列化格式，参数3：kafka配置信息
        //flink 自带序列化格式: SimpleStringSchema -- value 、TypeInformationKeyValueSerializationSchema -- <key, value>
        val consumer: FlinkKafkaConsumer[ConsumerRecord[String, String]] =
            new FlinkKafkaConsumer[ConsumerRecord[String, String]]("lll_cr", new MyDeserializationSchema(), kafka_property)

        //设置kafka读取数据的规则，此规则与kafka_property冲突时，以此规则为准
        //默认，将从kafka中找到消费者组消费的offset的位置，如果没有会按照auto.offset.reset 的配置策略
        consumer.setStartFromGroupOffsets()

        //设置flink 提交kafka offset的策略，当完成一批标记任务后（符合两阶段提交），将offset提交到kafka
        consumer.setCommitOffsetsOnCheckpoints(true)

        import org.apache.flink.streaming.api.scala._
        //添加数据源，获取对应flink的kafka数据源
        val kafka_source: DataStream[ConsumerRecord[String, String]] = env.addSource(consumer)

        //数据处理 trunsfrom
        val kafka_trunsfrom: DataStream[(String, String)] = kafka_source
                .map(_.value())
                .flatMap(value => {
                    if(value != null)
                        value.split(" ")
                    else Nil
                })
                .map((_, 1))
                .keyBy(0)
                .reduce((tu1, tu2) => (tu1._1, tu2._2 + tu1._2))
                .map(tu => (tu._1, s"${tu._1}"))

        //创建flink本版的kafka sink
        //参数1：topic，参数2：kafka数据的序列化类（目前没有默认），参数3：kafka配置信息（只需要broker），参数4：kafka提交数据方式
        //EXACTLY_ONCE：保证只正确的处理一次数据，AT_LEAST_ONCE：保证数据不会丢失，但可能出现重复处理数据，NONE：不做任何保证
        //FlinkKafkaProducer 有多种初始化方式，也可以使用flink自身实现序列化类的初始化方式（但已被废弃）
        //    new FlinkKafkaProducer[String]("broker:9092", "topic_id", new SimpleStringSchema())
        //    val producer = new FlinkKafkaProducer[(String, String)](
        //        "lll_pr",
        //        new MySerializationSchema("lll_pr"),
        //        kafka_property,
        //        FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
        //注意：生产者开启事务特性后，消费者在读取数据时，也需要开启事务特性，以此来过滤掉abort的数据（指定读模式为read_committed）
        val producer = new MyFlinkKafkaProducer(
            "lll_pr",
            new MySerializationSchema("lll_prr"),
            kafka_property,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
            "lll-node-02,lll-node-03,lll-node-04","2181"
        )

        //将处理后的数据写出到kafka
        kafka_trunsfrom.addSink(producer)

        //激活任务
        env.execute("lll job")

    }

}
