package lll.flink.base.sink.context

import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.collection.mutable.ListBuffer

class MyContextSer extends TypeSerializer[MyHbaseSinkContext] {

    private val serializer = new StringSerializer()

    //该类型是否为不可变类型
    override def isImmutableType: Boolean = true

    //该类型是否线程安全（是否是有状态的），线程安全的可以返回自身，否则深度复制并返回
    override def duplicate(): TypeSerializer[MyHbaseSinkContext] = this

    //创建一个新实例
    override def createInstance(): MyHbaseSinkContext = new MyHbaseSinkContext(Nil)

    //深度复制
    override def copy(from: MyHbaseSinkContext): MyHbaseSinkContext = new MyHbaseSinkContext(from.toSeq)

    //复制元素，如果reuse能够重用，则将from中的元素复制到reuse中并返回，否则进行深度复制
    override def copy(from: MyHbaseSinkContext, reuse: MyHbaseSinkContext): MyHbaseSinkContext = this.copy(from)

    //如果是固定长度类型，则返回数据类型的长度，否则返回-1
    override def getLength: Int = -1

    //序列化
    override def serialize(record: MyHbaseSinkContext, target: DataOutputView): Unit = {
        val size: Int = record.getLen
        target.writeInt(size)
        record.getMap.foreach(kv => {
            serializer.serialize(kv._1, target)
            if(kv._2 == null)
                target.writeBoolean(true)
            else {
                target.writeBoolean(false)
                serializer.serialize(kv._2, target)
            }
        })
    }

    //反序列化
    override def deserialize(source: DataInputView): MyHbaseSinkContext = {
        val size: Int = source.readInt()
        val seq = new ListBuffer[(String, String)]()
        for(_ <- 1 to size){
            val key: String = serializer.deserialize(source)
            val flag: Boolean = source.readBoolean()
            if(flag) seq.append((key, null))
            else seq.append((key, serializer.deserialize(source)))
        }
        new MyHbaseSinkContext(seq)
    }

    //反序列化，如果类型可变，则重用reuse，并返回，否则生成新的
    override def deserialize(reuse: MyHbaseSinkContext, source: DataInputView): MyHbaseSinkContext = this.deserialize(source)

    //拷贝序列化后数据
    override def copy(source: DataInputView, target: DataOutputView): Unit = {
        val size: Int = source.readInt()
        target.writeInt(size)
        for(_ <- 1 to size){
            serializer.copy(source, target)
            val flag: Boolean = source.readBoolean()
            target.writeBoolean(flag)
            if(!flag) serializer.copy(source, target)
        }
    }

    //为检查点生成快照
    override def snapshotConfiguration(): TypeSerializerSnapshot[MyHbaseSinkContext] = new MyHbaseSerializerSnapshot(this)

    def getSerializer = this.serializer

    override def hashCode(): Int = this.getClass.hashCode()

    override def equals(obj: scala.Any): Boolean = this.getClass.equals(obj.getClass)

}
