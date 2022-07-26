package net.choc.entry.plugins;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.api.Message;

public class FSchema implements PulsarDeserializationSchema<byte[]> {

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(new TypeHint<>() {});
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<byte[]> collector) throws Exception {
        collector.collect(message.getData());
    }
}
