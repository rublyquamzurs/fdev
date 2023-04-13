package net.choc.entry.framework;

import net.choc.entry.plugins.FSchema;
import net.choc.entry.sinks.FileSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.ByteSchema;

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

    public DataStream<String> SetParams() {
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

        return env.addSource(new MySource(), "Source", TypeInformation.of(String.class)).uid("test");
    }

    public void SetEnd(DataStream<String> bottleNeck) {
        Properties prop = new Properties();

        SinkFunction<String> sink = new FileSink();
        bottleNeck.addSink(sink);
    }

    public void Submit() throws Exception {
        env.execute(jobName);
    }
}
