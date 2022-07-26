package net.choc.entry.framework;

import net.choc.entry.plugins.FSchema;
import net.choc.entry.plugins.SinkSchema;
import net.choc.entry.plugins.SourceSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

public class Engine {
    private StreamExecutionEnvironment env;
    private final String jobName;

    public Engine() {
        jobName = "Choc";
    }

    public Engine(String jobName) {
        this.jobName =jobName;
    }

    public DataStream<byte[]> SetParams() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(10)));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(30L));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);

        Properties prop = new Properties();
        prop.setProperty(PulsarOptions.TOPIC_SINGLE_OPTION_KEY, "persistent://public/default/my_topic");
        prop.setProperty(PulsarOptions.ENABLE_KEY_HASH_RANGE_KEY, String.valueOf(true));
        prop.setProperty(PulsarOptions.SUBSCRIPTION_ROLE_OPTION_KEY, "NAE");

        PulsarSourceBuilder<byte[]> sourceBuilder = PulsarSource.builder()
            .setAdminUrl("http://192.168.131.129:32670")
            .setServiceUrl("pulsar://192.168.131.129:32669")
            .setTopics("persistent://public/default/my_topic")
            .setDeserializationSchema(new FSchema())
            .setSubscriptionType(SubscriptionType.Key_Shared)
            .setSubscriptionName("Test")
            .setStartCursor(StartCursor.latest())
            .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, true);

//        DurableSource<byte[]> source =
//            new DurableSource<>(
//                "pulsar://192.168.131.129:32669",
//                "http://192.168.131.129:32670",
//                new SourceSchema(), prop)
//                .setStartFromSubscription("test", MessageId.latest)
//                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
        return env.fromSource(sourceBuilder.build(), WatermarkStrategy.noWatermarks(), "Source").uid("test");
    }

    public void SetEnd(DataStream<byte[]> bottleNeck) {
        Properties prop = new Properties();

        SinkFunction<byte[]> sink = new FlinkPulsarSink<>(
                "pulsar://192.168.131.129:32669",
                "http://192.168.131.129:32670",
                Optional.of("persistent://public/default/my_topic_out"),
                prop, new SinkSchema());
        bottleNeck.addSink(sink);
    }

    public void Submit() throws Exception {
        env.execute(jobName);
    }
}
