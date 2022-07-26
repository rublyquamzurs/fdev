package net.choc.entry;

import net.choc.entry.framework.Engine;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        Engine engine = new Engine("yestest");

        DataStream<byte[]> ori = engine.SetParams();
        DataStream<byte[]> mid = ori.assignTimestampsAndWatermarks(WatermarkStrategy
            .<byte[]>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((e, t) -> System.currentTimeMillis()));
        DataStream<byte[]> bottle = mid.keyBy((KeySelector<byte[], Object>) bytes -> "").process(new KeyedProcessFunction<>() {
            int count = 0;

            @Override
            public void processElement(byte[] bytes, KeyedProcessFunction<Object, byte[], byte[]>.Context context, Collector<byte[]> collector) throws Exception {
                String info = new String(bytes);

                if (info.equals("stop")) {
                    count++;
                    if (count > 1)
                        throw new FlinkRuntimeException("exit");
                }
                System.out.println(info);
                collector.collect((info + " mark").getBytes(StandardCharsets.UTF_8));
            }
        });
        engine.SetEnd(bottle);

        engine.Submit();
    }
}
