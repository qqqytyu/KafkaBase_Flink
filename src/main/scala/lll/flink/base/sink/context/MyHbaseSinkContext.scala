package lll.flink.base.sink.context

import scala.collection.mutable

class MyHbaseSinkContext(kv: Seq[(String, String)]) extends Serializable {

    private val context_text = kv.toMap

    def getValue(key: String): String = context_text.getOrElse(key, "")

    def toSeq: Seq[(String, String)] = context_text.toList

    def getLen:Int = context_text.size

    def getMap = context_text

}
