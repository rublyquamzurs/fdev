package net.choc.entry.dao;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public final class MyStateSerializer extends TypeSerializer<MyState> {

    public MyStateSerializer() {}

    // 是否是不可变类型
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<MyState> duplicate() {
        return new MyStateSerializer();
    }

    @Override
    public MyState createInstance() {
        return new MyState();
    }

    // 拷贝新的出来
    @Override
    public MyState copy(MyState myState) {
        MyState rState = new MyState();
        rState.setI1(myState.getI1());
        rState.setI2(myState.getI2());
        rState.setI3(myState.getI3());
        rState.setI4(myState.getI4());
        return rState;
    }

    // 拷贝到现有内存
    @Override
    public MyState copy(MyState myState, MyState t1) {
        if (t1 == null)
            return null;
        t1.setI1(myState.getI1());
        t1.setI2(myState.getI2());
        t1.setI3(myState.getI3());
        t1.setI4(myState.getI4());
        return t1;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(MyState myState, DataOutputView dataOutputView) throws IOException {
        dataOutputView.write(myState.getI1());
        dataOutputView.write(myState.getI2());
        dataOutputView.write(myState.getI3());
        dataOutputView.write(myState.getI4());
    }

    @Override
    public MyState deserialize(DataInputView dataInputView) throws IOException {
        MyState state = new MyState();
        state.setI1(dataInputView.readInt());
        state.setI2(dataInputView.readInt());
        state.setI3(dataInputView.readInt());
        state.setI4(dataInputView.readInt());
        return state;
    }

    // 反序列化到现有内存
    @Override
    public MyState deserialize(MyState myState, DataInputView dataInputView) throws IOException {
        if (myState == null)
            return null;
        myState.setI1(dataInputView.readInt());
        myState.setI2(dataInputView.readInt());
        myState.setI3(dataInputView.readInt());
        myState.setI4(dataInputView.readInt());
        return myState;
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
        dataOutputView.write(dataInputView.readInt());
        dataOutputView.write(dataInputView.readInt());
        dataOutputView.write(dataInputView.readInt());
        dataOutputView.write(dataInputView.readInt());
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<MyState> snapshotConfiguration() {
        return new MyStateTypeSerializerSnapshot();
    }
}
