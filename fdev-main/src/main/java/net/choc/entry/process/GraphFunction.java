package net.choc.entry.process;

import net.choc.entry.dao.MyState;
import net.choc.entry.dao.MyStateSerializer;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class GraphFunction extends KeyedProcessFunction<String, String, String> {
    public GraphFunction() {}

    ValueState<MyState> myValStat;

    @Override
    public void open(Configuration configuration) {
        ValueStateDescriptor<MyState> dest = new ValueStateDescriptor<>("myState", new MyStateSerializer());
    }


    @Override
    public void processElement(String s, KeyedProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
        collector.collect("handle: " + s);
    }

    @Override
    public void onTimer(
        long timestamp,
        KeyedProcessFunction<String, String, String>.OnTimerContext context, Collector<String> collector) {

    }
}
