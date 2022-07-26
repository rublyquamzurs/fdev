package net.choc.entry.plugins;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarSerializationSchema;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public class SinkSchema implements PulsarSerializationSchema<byte[]> {
    private final byte[] mark = new byte[]{'A', 'B', 'C'};

    @Override
    public void serialize(byte[] bytes, TypedMessageBuilder<byte[]> typedMessageBuilder) {
        typedMessageBuilder.keyBytes(mark);
        typedMessageBuilder.value(bytes);
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
