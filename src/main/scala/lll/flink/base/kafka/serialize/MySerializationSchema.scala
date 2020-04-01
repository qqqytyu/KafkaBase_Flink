package lll.flink.base.kafka.serialize

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 自定义kafka序列化类，需要 K,V格式的数据
 * @param topic 目标topic
 * @param charset 序列化编码方式，默认UTF-8
 */
class MySerializationSchema(topic: String, charset: String = "UTF-8") extends KafkaSerializationSchema[(String, String)]{

    override def serialize(element: (String, String), timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {

        var key_b: Array[Byte] = null

        var value_b: Array[Byte] = null

        if(element._1 != null && element._1.nonEmpty)
            key_b = element._1.getBytes(charset)

        if(element._2 != null && element._2.nonEmpty)
            value_b = element._2.getBytes(charset)
        else value_b = "".getBytes(charset)

        if(key_b == null)
            new ProducerRecord(topic, value_b)
        else
            new ProducerRecord(topic, key_b, value_b)

    }

}
