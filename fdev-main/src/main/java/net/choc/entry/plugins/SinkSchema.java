package net.choc.entry.plugins;

import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;

public class SinkSchema implements PulsarSerializationSchema<byte[]> {
    private final byte[] mark = new byte[]{'A', 'B', 'C'};

    @Override
    public PulsarMessage<?> serialize(byte[] bytes, PulsarSinkContext pulsarSinkContext) {
        return null;
    }
}
