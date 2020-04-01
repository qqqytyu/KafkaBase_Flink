package lll.flink.base.kafka

import java.util.Properties

import lll.flink.base.source.MyIbmMqSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ReadMqToConsole {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)

        //配置MQ运行参数
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

        val mqSource = new MyIbmMqSource(kafka_property)

        import org.apache.flink.streaming.api.scala._
        val source: DataStream[String] = env.addSource(mqSource)

        val ds: DataStream[String] = source.flatMap(_.split(" "))

        ds.print()

        env.execute("mq")

    }

}
