package net.choc.entry.framework;

import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarDeserializationSchema;

import java.util.Properties;

public class MySource<T> extends FlinkPulsarSource<T> {
    public MySource(String serviceUrl, String adminUrl, PulsarDeserializationSchema<T> deserializer, Properties properties) {
        super(serviceUrl, adminUrl, deserializer, properties);
    }
}
