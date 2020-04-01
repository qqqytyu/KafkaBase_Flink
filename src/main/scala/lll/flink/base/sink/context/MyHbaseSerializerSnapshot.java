package lll.flink.base.sink.context;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

/**
 * 重载Flink的快照方法，由于需要不同子类构造来调用不同父类构造，这种情况目前scala无法实现，故采用java实现
 */
public class MyHbaseSerializerSnapshot extends CompositeTypeSerializerSnapshot<MyHbaseSinkContext, MyContextSer> {

    private static final int CURRENT_VERSION = 1;

    //恢复快照时调用
    public MyHbaseSerializerSnapshot() {
        super(MyContextSer.class);
    }

    //生成快照时调用
    public MyHbaseSerializerSnapshot(MyContextSer serializerInstance) {
        super(serializerInstance);
    }

    //使用当前快照写入数据时，写入的版本号
    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    //如果此序列化类中有嵌套序列化类，则通过nestedSerializers数组来生成最外层MyContextSer序列化类
    @Override
    protected MyContextSer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        return new MyContextSer();
    }

    //从最外层序列化类获取内层序列化类
    @Override
    protected TypeSerializer<?>[] getNestedSerializers(MyContextSer outerSerializer) {
        return new TypeSerializer<?>[] {outerSerializer.getSerializer()};
    }

}
