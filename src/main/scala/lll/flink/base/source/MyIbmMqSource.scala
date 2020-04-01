package lll.flink.base.source

import java.util.Properties

import com.ibm.mq.constants.MQConstants
import com.ibm.mq.{MQEnvironment, MQException, MQGetMessageOptions, MQMessage, MQQueue, MQQueueManager}
import lll.flink.base.source.MyIbmMqSource.IbmMqConnect
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
 * 实现多并发的读取IBM MQ队列中的数据，并发数量和Flink的并行数相关
 * ParallelSourceFunction -- 单线程产生数据，与Flink的并行度无关
 * RichParallelSourceFunction -- 多线程产生数据，与Flink的并行度相关
 */
class MyIbmMqSource(mq_property: Properties) extends RichParallelSourceFunction[String]{

    var ibmMqConnect: IbmMqConnect = _

    //第一步：调用open方法，可以加载配置文件或者一些资源
    override def open(parameters: Configuration): Unit = {
        ibmMqConnect = new IbmMqConnect(mq_property)
        super.open(parameters)
    }

    //第二步：调用run方法产生数据，启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        try {
            while (!Thread.interrupted()){
                ibmMqConnect.receiveOneMsg() match {
                    case Some(value) => ctx.collect(value)
                    case _ => {
                        Thread.sleep(1000 * 5)
                        MyIbmMqSource.logger.warn("未读取到任何数据...")
                    }
                }
            }
        } catch {
            case ex: Exception => MyIbmMqSource.logger.error(ex.getMessage, ex)
        } finally {
            ibmMqConnect.close()
            MyIbmMqSource.logger.warn("线程已被中断，source源准备退出...")
        }
    }

    //第三步：当"取消"事件被触发时，会先执行cancel方法
    override def cancel(): Unit = {
        MyIbmMqSource.logger.info("当前任务已被取消，准备退出...")
    }

    //第四步：当cancel方法执行完成后，会执行close方法
    override def close(): Unit = {
        super.close()
    }

}

object MyIbmMqSource{

    private val logger: Logger = LoggerFactory.getLogger(MyIbmMqSource.getClass)

    class IbmMqConnect(mq_property: Properties) extends Serializable {

        val queueManagerName:String = mq_property.getProperty("qmName")

        val queueName:String = mq_property.getProperty("qName")

        //在获取MQ消息时，如果没有数据，等待的时间-毫秒
        val waitInt:Int = mq_property.getProperty("waitInt").toInt

        //获取ccsid对应的字符编码
        val charsetName: String = getcharacter(mq_property.getProperty("ccsid"))

        @transient
        var qMgr:MQQueueManager = _

        mqInit()

        private def mqInit(): Unit ={

            //MQ服务器地址
            MQEnvironment.hostname = mq_property.getProperty("hostname")

            //MQ服务器对应端口号
            MQEnvironment.port = mq_property.getProperty("port").toInt

            //订阅的主题名称
            MQEnvironment.channel = mq_property.getProperty("channel")

            //队列数据的编码方式
            MQEnvironment.CCSID = mq_property.getProperty("ccsid").toInt

            //用户名
            MQEnvironment.userID = mq_property.getProperty("userID")

            //密码
            MQEnvironment.password = mq_property.getProperty("password")

            qMgr = new MQQueueManager(queueManagerName)

        }

        def getMQQueue: MQQueue ={

            //设置将要连接的队列属性
            val openOptions: Int =
                MQConstants.getIntValue("MQOO_INPUT_AS_Q_DEF") |
                        MQConstants.getIntValue("MQOO_OUTPUT") |
                        MQConstants.getIntValue("MQOO_INQUIRE")

            //如果与mq的连接关闭，就重新打开
            if (qMgr == null || !qMgr.isConnected()) mqInit

            qMgr.accessQueue(queueName, openOptions)

        }

        def getMessageOptions: MQGetMessageOptions ={

            val gmo: MQGetMessageOptions = new MQGetMessageOptions()

            //在同步点控制下获取消息, 读取消息成功后，需要MQQueueManager提交事务(commit)才会真正的成功消费数据
            gmo.options = gmo.options + MQConstants.getIntValue("MQGMO_SYNCPOINT")

            //如果在队列上没有消息则等待
            gmo.options = gmo.options + MQConstants.getIntValue("MQGMO_FAIL_IF_QUIESCING")

            //设置等待的毫秒时间限制,超过时间没有数据则失败
            gmo.waitInterval = waitInt

            gmo

        }

        private def getcharacter: PartialFunction[String, String] ={
            case "1208" => "UTF-8"
            case "1381" => "GBK"
            case "1392" => "GB18030"
            case "819" => "ISO-8859-1"
        }

        //读取一条数据
        def receiveOneMsg(queue: MQQueue = getMQQueue, gmo: MQGetMessageOptions = getMessageOptions, autoClose: Boolean = true): Option[String] ={

            try {

                // 要读的队列的消息
                val retrieve: MQMessage = new MQMessage()

                // 从队列中取出消息
                queue.get(retrieve, gmo)

                val bytes: Array[Byte] = new Array[Byte](retrieve.getMessageLength)

                retrieve.readFully(bytes)

                Some(new String(bytes, charsetName))

            } catch {
                case ex: MQException if ex.reasonCode == 2033 => None
                case ex => throw ex
            } finally if(autoClose) commit(queue)

        }

        //读取MQ中数据，一次取num条数据
        def receiveMsg(num:Int): ListBuffer[Option[String]] ={

            val queue: MQQueue = getMQQueue

            val gmo: MQGetMessageOptions = getMessageOptions

            //当前队列深度
            var depth:Int = queue.getCurrentDepth

            //一次最多取num条
            if(num != -1 && depth >= num) depth = num

            val ss = new ListBuffer[Option[String]]()

            (1 to depth).foreach(_ => ss.append(receiveOneMsg(queue, gmo, false)))

            commit(queue)

            ss

        }

        def commit(queue: MQQueue): Unit ={
            if(qMgr != null && qMgr.isConnected())
                qMgr.commit()
            queue.close()
        }

        def close(): Unit ={

            qMgr.disconnect()

            qMgr = null

        }

    }

}
