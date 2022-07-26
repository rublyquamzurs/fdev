package net.choc.entry;

import net.choc.entry.framework.Engine;
import net.choc.entry.process.GraphFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        Engine engine = new Engine("yestest");

        DataStream<String> ori = engine.SetParams();
        DataStream<String> mid = ori.assignTimestampsAndWatermarks(WatermarkStrategy
            .<String>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((e, t) -> System.currentTimeMillis()));
        DataStream<String> bottle = mid
            .keyBy((KeySelector<String, String>) bytes -> "")
            .process(new GraphFunction());
        engine.SetEnd(bottle);

        engine.Submit();
    }
}
