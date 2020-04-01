import java.util.Properties

import com.ibm.mq.{MQGetMessageOptions, MQMessage, MQQueue}
import lll.flink.base.source
import lll.flink.base.source.MyIbmMqSource

object MqTest {

    def main(args: Array[String]): Unit = {

        val kafka_property: Properties = new Properties()

        kafka_property.setProperty("hostname","192.168.1.125")
        kafka_property.setProperty("port","1414")
        kafka_property.setProperty("userID","mqm")
        kafka_property.setProperty("password","mqm")
        kafka_property.setProperty("channel","CHANNEL.ADMIN.TEST")
        kafka_property.setProperty("qmName","lll_flink")
        kafka_property.setProperty("qName","QNAME.TEST")
        kafka_property.setProperty("ccsid","1208")
        kafka_property.setProperty("waitInt","5000")

        val connect = new source.MyIbmMqSource.IbmMqConnect(kafka_property)

        println(connect.receiveOneMsg())

        val queue: MQQueue = connect.getMQQueue

        val options: MQGetMessageOptions = connect.getMessageOptions

        val retrieve: MQMessage = new MQMessage()

        queue.get(retrieve, options)

        println(retrieve.getDataOffset)
        println(retrieve.offset)

        val bytes: Array[Byte] = new Array[Byte](retrieve.getMessageLength)

        retrieve.readFully(bytes)



        connect.close()

    }

}
