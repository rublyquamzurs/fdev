package net.choc.entry.plugins;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarDeserializationSchema;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import java.io.IOException;


public class SourceSchema implements PulsarDeserializationSchema<byte[]> {
    @Override
    public boolean isEndOfStream(byte[] bytes) {
        return false;
    }

    @Override
    public byte[] deserialize(Message<byte[]> message) throws IOException {
        return message.getData();
    }

    @Override
    public Schema<byte[]> getSchema() {
        return Schema.BYTES;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {});
    }
}
