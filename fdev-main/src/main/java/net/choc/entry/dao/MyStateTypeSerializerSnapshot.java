package net.choc.entry.dao;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class MyStateTypeSerializerSnapshot implements TypeSerializerSnapshot<MyState> {

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {

    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

    }

    @Override
    public TypeSerializer<MyState> restoreSerializer() {
        return new MyStateSerializer();
    }

    @Override
    public TypeSerializerSchemaCompatibility<MyState> resolveSchemaCompatibility(TypeSerializer<MyState> newSerializer) {
        if (newSerializer.snapshotConfiguration().getCurrentVersion() == this.getCurrentVersion())
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        if (newSerializer.snapshotConfiguration().getCurrentVersion() >= this.getCurrentVersion())
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
