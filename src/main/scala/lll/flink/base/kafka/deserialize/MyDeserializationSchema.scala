package lll.flink.base.kafka.deserialize

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * 自定义kafka反序列化类，添加额外的topic、offset、partition等信息
 * @param charset 编码字符集
 */
class MyDeserializationSchema(charset: String = "UTF-8") extends KafkaDeserializationSchema[ConsumerRecord[String, String]] {

    //连接的是kafka队列，故正常情况下，并没有结束标识，此方法总返回false
    override def isEndOfStream(nextElement: ConsumerRecord[String, String]): Boolean = false

    //将获取到kafka序列化信息进行反序列化，并添加一些额外参数
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ConsumerRecord[String, String] = {

        var key: String = null

        var value: String = null

        if(record.key() != null && record.key().nonEmpty)
            key = new String(record.key(), charset)

        if(record.value() != null && record.value().nonEmpty)
            value = new String(record.value(), charset)

        new ConsumerRecord[String, String](
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.checksum,
            record.serializedKeySize,
            record.serializedValueSize(),
            key, value)

    }

    override def getProducedType: TypeInformation[ConsumerRecord[String, String]] =
        TypeInformation.of(new TypeHint[ConsumerRecord[String, String]]{})

}
