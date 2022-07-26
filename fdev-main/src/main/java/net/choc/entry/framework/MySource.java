package net.choc.entry.framework;

import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class MySource implements SourceFunction<String> {
    public MySource() {
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int count = 0;
        for (int i = 0; i < 10; ++i) {
            ctx.collect(Integer.toString(i));
        }
    }

    @Override
    public void cancel() {

    }
}
