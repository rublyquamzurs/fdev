package net.choc.entry.sources;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;

public class MySource extends SourceReaderBase {
    public MySource(FutureCompletingBlockingQueue elementsQueue, SplitFetcherManager splitFetcherManager, RecordEmitter recordEmitter, Configuration config, SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map map) {

    }

    @Override
    protected Object initializedState(SourceSplit sourceSplit) {
        return null;
    }

    @Override
    protected SourceSplit toSplitType(String s, Object o) {
        return null;
    }
}
