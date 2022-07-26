package net.choc.entry.framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.pulsar.config.StartupMode;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.MessageIdSerializer;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCommitCallback;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarFetcher;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarMetadataReader;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarSourceStateSerializer;
import org.apache.flink.streaming.connectors.pulsar.internal.SerializableRange;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicSubscription;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicSubscriptionSerializer;
import org.apache.flink.streaming.connectors.pulsar.serialization.PulsarDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.shade.com.google.common.collect.Maps;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DurableSource<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointListener, CheckpointedFunction {
    private static final Logger logger = LoggerFactory.getLogger(DurableSource.class);
    private static final long serialVersionUID = -6080350107046202906L;
    protected String adminUrl;
    protected ClientConfigurationData clientConfigurationData;
    protected final Map<String, String> caseInsensitiveParams;
    protected final Map<String, Object> readerConf;
    protected volatile PulsarDeserializationSchema<T> deserializer;
    private Map<TopicRange, MessageId> ownedTopicStarts;
    private SerializedValue<WatermarkStrategy<T>> watermarkStrategy;
    private final long discoveryIntervalMillis;
    protected final int pollTimeoutMs;
    protected final int commitMaxRetries;
    private StartupMode startupMode;
    private transient Map<TopicRange, MessageId> specificStartupOffsets;
    private String externalSubscriptionName;
    private MessageId subscriptionPosition;
    private Map<TopicRange, byte[]> specificStartupOffsetsAsBytes;
    protected final Properties properties;
    protected final UUID uuid;
    private final LinkedHashMap<Long, Map<TopicRange, MessageId>> pendingOffsetsToCommit;
    private transient volatile PulsarFetcher<T> pulsarFetcher;
    protected transient volatile PulsarMetadataReader metadataReader;
    private transient volatile TreeMap<TopicRange, MessageId> restoredState;
    private transient volatile Set<TopicRange> excludeStartMessageIds;
    private transient ListState<Tuple2<TopicSubscription, MessageId>> unionOffsetStates;
    private int oldStateVersion;
    private volatile boolean stateSubEqualexternalSub;
    private transient volatile Thread discoveryLoopThread;
    private volatile boolean running;
    private final boolean useMetrics;
    private transient Counter successfulCommits;
    private transient Counter failedCommits;
    private transient PulsarCommitCallback offsetCommitCallback;
    private transient int taskIndex;
    private transient int numParallelTasks;
    private long startupOffsetsTimestamp;

    public DurableSource(String adminUrl, ClientConfigurationData clientConf, PulsarDeserializationSchema<T> deserializer, Properties properties) {
        this.startupMode = StartupMode.LATEST;
        this.subscriptionPosition = MessageId.latest;
        this.uuid = UUID.randomUUID();
        this.pendingOffsetsToCommit = new LinkedHashMap();
        this.oldStateVersion = 2;
        this.stateSubEqualexternalSub = false;
        this.running = true;
        this.startupOffsetsTimestamp = -1L;
        this.adminUrl = (String)Preconditions.checkNotNull(adminUrl);
        this.clientConfigurationData = (ClientConfigurationData)Preconditions.checkNotNull(clientConf);
        this.deserializer = deserializer;
        this.properties = properties;
        this.caseInsensitiveParams = SourceSinkUtils.validateStreamSourceOptions(Maps.fromProperties(properties));
        this.readerConf = SourceSinkUtils.getReaderParams(Maps.fromProperties(properties));
        this.discoveryIntervalMillis = SourceSinkUtils.getPartitionDiscoveryIntervalInMillis(this.caseInsensitiveParams);
        this.pollTimeoutMs = SourceSinkUtils.getPollTimeoutMs(this.caseInsensitiveParams);
        this.commitMaxRetries = SourceSinkUtils.getCommitMaxRetries(this.caseInsensitiveParams);
        this.useMetrics = SourceSinkUtils.getUseMetrics(this.caseInsensitiveParams);
        CachedPulsarClient.setCacheSize(SourceSinkUtils.getClientCacheSize(this.caseInsensitiveParams));
        if (this.clientConfigurationData.getServiceUrl() == null) {
            throw new IllegalArgumentException("ServiceUrl must be supplied in the client configuration");
        } else {
            this.oldStateVersion = SourceSinkUtils.getOldStateVersion(this.caseInsensitiveParams, this.oldStateVersion);
        }
    }

    public DurableSource(String serviceUrl, String adminUrl, PulsarDeserializationSchema<T> deserializer, Properties properties) {
        this(adminUrl, PulsarClientUtils.newClientConf((String)Preconditions.checkNotNull(serviceUrl), properties), deserializer, properties);
    }

    public DurableSource<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy) {
        Preconditions.checkNotNull(watermarkStrategy);

        try {
            ClosureCleaner.clean(watermarkStrategy, ClosureCleanerLevel.RECURSIVE, true);
            this.watermarkStrategy = new SerializedValue(watermarkStrategy);
            return this;
        } catch (Exception var3) {
            throw new IllegalArgumentException("The given WatermarkStrategy is not serializable", var3);
        }
    }

    public DurableSource<T> setStartFromEarliest() {
        this.startupMode = StartupMode.EARLIEST;
        this.specificStartupOffsets = null;
        return this;
    }

    public DurableSource<T> setStartFromLatest() {
        this.startupMode = StartupMode.LATEST;
        this.specificStartupOffsets = null;
        return this;
    }

    public DurableSource<T> setStartFromSpecificOffsets(Map<String, MessageId> specificStartupOffsets) {
        Preconditions.checkNotNull(specificStartupOffsets);
        this.specificStartupOffsets = (Map)specificStartupOffsets.entrySet().stream().collect(Collectors.toMap((e) -> {
            return new TopicRange((String)e.getKey());
        }, Map.Entry::getValue));
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        this.specificStartupOffsetsAsBytes = new HashMap();
        Iterator var2 = this.specificStartupOffsets.entrySet().iterator();

        while(var2.hasNext()) {
            Map.Entry<TopicRange, MessageId> entry = (Map.Entry)var2.next();
            this.specificStartupOffsetsAsBytes.put(entry.getKey(), ((MessageId)entry.getValue()).toByteArray());
        }

        return this;
    }

    public DurableSource<T> setStartFromSubscription(String externalSubscriptionName) {
        this.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
        this.externalSubscriptionName = (String)Preconditions.checkNotNull(externalSubscriptionName);
        return this;
    }

    public DurableSource<T> setStartFromSubscription(String externalSubscriptionName, MessageId subscriptionPosition) {
        this.startupMode = StartupMode.EXTERNAL_SUBSCRIPTION;
        this.externalSubscriptionName = (String)Preconditions.checkNotNull(externalSubscriptionName);
        this.subscriptionPosition = (MessageId)Preconditions.checkNotNull(subscriptionPosition);
        return this;
    }

    public DurableSource<T> setStartFromTimestamp(long startupOffsetsTimestamp) {
        Preconditions.checkArgument(startupOffsetsTimestamp >= 0L, "The provided value for the startup offsets timestamp is invalid.");
        long currentTimestamp = System.currentTimeMillis();
        Preconditions.checkArgument(startupOffsetsTimestamp <= currentTimestamp, "Startup time[%s] must be before current time[%s].", new Object[]{startupOffsetsTimestamp, currentTimestamp});
        this.startupMode = StartupMode.TIMESTAMP;
        this.startupOffsetsTimestamp = startupOffsetsTimestamp;
        this.specificStartupOffsets = null;
        return this;
    }

    public void open(Configuration parameters) throws Exception {
        if (this.deserializer != null) {
            this.deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(this.getRuntimeContext(), (metricGroup) -> {
                return metricGroup.addGroup("user");
            }));
        }

        this.taskIndex = this.getRuntimeContext().getIndexOfThisSubtask();
        this.numParallelTasks = this.getRuntimeContext().getNumberOfParallelSubtasks();
        this.metadataReader = this.createMetadataReader();
        this.ownedTopicStarts = new HashMap();
        this.excludeStartMessageIds = new HashSet();
        Set<TopicRange> allTopics = this.metadataReader.discoverTopicChanges();
        if (this.specificStartupOffsets == null && this.specificStartupOffsetsAsBytes != null) {
            this.specificStartupOffsets = new HashMap();
            Iterator var3 = this.specificStartupOffsetsAsBytes.entrySet().iterator();

            while(var3.hasNext()) {
                Map.Entry<TopicRange, byte[]> entry = (Map.Entry)var3.next();
                this.specificStartupOffsets.put(entry.getKey(), MessageId.fromByteArray((byte[])entry.getValue()));
            }
        }
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://192.168.131.129:32670").build();

        Map<TopicRange, MessageId> allTopicOffsets = this.offsetForEachTopic(allTopics, this.startupMode, this.specificStartupOffsets);
        boolean usingRestoredState = this.startupMode != StartupMode.EXTERNAL_SUBSCRIPTION || this.stateSubEqualexternalSub;
        if (this.restoredState != null && usingRestoredState) {
            allTopicOffsets.entrySet().stream().filter((e) -> {
                return !this.restoredState.containsKey(e.getKey());
            }).forEach((e) -> {
                MessageId var10000 = (MessageId)this.restoredState.put(e.getKey(), e.getValue());
            });
            SerializableRange subTaskRange = this.metadataReader.getRange();
            this.restoredState.entrySet().stream().filter((e) -> {
                return SourceSinkUtils.belongsTo(((TopicRange)e.getKey()).getTopic(), subTaskRange, this.numParallelTasks, this.taskIndex);
            }).forEach((e) -> {
                TopicRange tr = new TopicRange(((TopicRange)e.getKey()).getTopic(), subTaskRange.getPulsarRange());
                this.ownedTopicStarts.put(tr, e.getValue());
                this.excludeStartMessageIds.add(e.getKey());
            });
            Set<TopicRange> goneTopics = (Set)Sets.difference(this.restoredState.keySet(), allTopics).stream().filter((k) -> {
                return SourceSinkUtils.belongsTo(k.getTopic(), subTaskRange, this.numParallelTasks, this.taskIndex);
            }).map((k) -> {
                return new TopicRange(k.getTopic(), subTaskRange.getPulsarRange());
            }).collect(Collectors.toSet());
            Iterator var7 = goneTopics.iterator();

            while(var7.hasNext()) {
                TopicRange goneTopic = (TopicRange)var7.next();
                logger.warn(goneTopic + " is removed from subscription since it no longer matches with topics settings.");
                this.ownedTopicStarts.remove(goneTopic);
            }

            logger.info("Source {} will start reading {} topics in restored state {}", new Object[]{this.taskIndex, this.ownedTopicStarts.size(), StringUtils.join(new Set[]{this.ownedTopicStarts.entrySet()})});
        } else {
            this.ownedTopicStarts.putAll((Map)allTopicOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            if (this.ownedTopicStarts.isEmpty()) {
                logger.info("Source {} initially has no topics to read from.", this.taskIndex);
            } else {
                logger.info("Source {} will start reading {} topics from initialized positions: {}", new Object[]{this.taskIndex, this.ownedTopicStarts.size(), this.ownedTopicStarts});
            }
        }

    }

    protected String getSubscriptionName() {
        if (this.startupMode == StartupMode.EXTERNAL_SUBSCRIPTION) {
            Preconditions.checkNotNull(this.externalSubscriptionName);
            return this.externalSubscriptionName;
        } else {
            return "flink-pulsar-" + this.uuid.toString();
        }
    }

    protected PulsarMetadataReader createMetadataReader() throws PulsarClientException {
        return new PulsarMetadataReader(this.adminUrl, this.clientConfigurationData, this.getSubscriptionName(), this.caseInsensitiveParams, this.taskIndex, this.numParallelTasks, this.startupMode == StartupMode.EXTERNAL_SUBSCRIPTION);
    }

    public void run(SourceFunction.SourceContext<T> ctx) throws Exception {
        if (this.ownedTopicStarts == null) {
            throw new Exception("The partitions were not set for the source");
        } else {
            this.successfulCommits = this.getRuntimeContext().getMetricGroup().counter("commitsSucceeded");
            this.failedCommits = this.getRuntimeContext().getMetricGroup().counter("commitsFailed");
            this.offsetCommitCallback = new PulsarCommitCallback() {
                public void onSuccess() {
                    DurableSource.this.successfulCommits.inc();
                }

                public void onException(Throwable cause) {
                    logger.warn("source {} failed commit by {}", DurableSource.this.taskIndex, cause.toString());
                    DurableSource.this.failedCommits.inc();
                }
            };
            if (this.ownedTopicStarts.isEmpty()) {
                ctx.markAsTemporarilyIdle();
            }

            logger.info("Source {} creating fetcher with offsets {}", this.taskIndex, StringUtils.join(new Set[]{this.ownedTopicStarts.entrySet()}));
            StreamingRuntimeContext streamingRuntime = (StreamingRuntimeContext)this.getRuntimeContext();
            this.pulsarFetcher = this.createFetcher(ctx, this.ownedTopicStarts, this.watermarkStrategy, streamingRuntime.getProcessingTimeService(), streamingRuntime.getExecutionConfig().getAutoWatermarkInterval(), this.getRuntimeContext().getUserCodeClassLoader(), streamingRuntime, this.useMetrics, this.excludeStartMessageIds);
            if (this.running) {
                if (this.discoveryIntervalMillis < 0L) {
                    this.pulsarFetcher.runFetchLoop();
                } else {
                    this.runWithTopicsDiscovery();
                }

            }
        }
    }

    protected PulsarFetcher<T> createFetcher(SourceFunction.SourceContext<T> sourceContext, Map<TopicRange, MessageId> seedTopicsWithInitialOffsets, SerializedValue<WatermarkStrategy<T>> watermarkStrategy, ProcessingTimeService processingTimeProvider, long autoWatermarkInterval, ClassLoader userCodeClassLoader, StreamingRuntimeContext streamingRuntime, boolean useMetrics, Set<TopicRange> excludeStartMessageIds) throws Exception {
        return new PulsarFetcher(sourceContext, seedTopicsWithInitialOffsets, excludeStartMessageIds, watermarkStrategy, processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, streamingRuntime, this.clientConfigurationData, this.readerConf, this.pollTimeoutMs, this.commitMaxRetries, this.deserializer, this.metadataReader, streamingRuntime.getMetricGroup().addGroup("PulsarConsumer"), useMetrics, this.startupOffsetsTimestamp);
    }

    public void joinDiscoveryLoopThread() throws InterruptedException {
        if (this.discoveryLoopThread != null) {
            this.discoveryLoopThread.join();
        }

    }

    public void runWithTopicsDiscovery() throws Exception {
        AtomicReference<Exception> discoveryLoopErrorRef = new AtomicReference();
        this.createAndStartDiscoveryLoop(discoveryLoopErrorRef);
        this.pulsarFetcher.runFetchLoop();
        this.joinDiscoveryLoopThread();
        Exception discoveryLoopError = (Exception)discoveryLoopErrorRef.get();
        if (discoveryLoopError != null) {
            throw new IllegalStateException(discoveryLoopError);
        }
    }

    private void createAndStartDiscoveryLoop(AtomicReference<Exception> discoveryLoopErrorRef) {
        this.discoveryLoopThread = new Thread(() -> {
            while(true) {
                try {
                    if (this.running) {
                        Set<TopicRange> added = this.metadataReader.discoverTopicChanges();
                        if (this.running && !added.isEmpty()) {
                            this.pulsarFetcher.addDiscoveredTopics(added);
                        }

                        if (this.running && this.discoveryIntervalMillis != -1L) {
                            Thread.sleep(this.discoveryIntervalMillis);
                        }
                        continue;
                    }
                } catch (PulsarMetadataReader.ClosedException var8) {
                } catch (InterruptedException var9) {
                } catch (Exception var10) {
                    discoveryLoopErrorRef.set(var10);
                } finally {
                    if (this.running) {
                        this.cancel();
                    }

                }

                return;
            }
        }, "Pulsar topic discovery for source " + this.taskIndex);
        this.discoveryLoopThread.start();
    }

    public void close() throws Exception {
        this.cancel();
        this.joinDiscoveryLoopThread();
        Exception exception = null;
        if (this.metadataReader != null) {
            try {
                this.metadataReader.close();
            } catch (Exception var4) {
                exception = var4;
            }
        }

        try {
            super.close();
        } catch (Exception var3) {
            exception = (Exception)ExceptionUtils.firstOrSuppressed(var3, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    public void cancel() {
        this.running = false;
        if (this.discoveryLoopThread != null) {
            this.discoveryLoopThread.interrupt();
        }

        if (this.pulsarFetcher != null) {
            try {
                this.pulsarFetcher.cancel();
            } catch (Exception var2) {
                logger.error("Failed to cancel the Pulsar Fetcher {}", ExceptionUtils.stringifyException(var2));
                throw new IllegalStateException(var2);
            }
        }

    }

    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        this.unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor("topic-offset-states", createStateSerializer()));
        if (context.isRestored()) {
            this.restoredState = new TreeMap();
            Iterator<Tuple2<TopicSubscription, MessageId>> iterator = ((Iterable)this.unionOffsetStates.get()).iterator();
            if (!iterator.hasNext()) {
                iterator = this.tryMigrateState(stateStore);
            }

            while(iterator.hasNext()) {
                Tuple2<TopicSubscription, MessageId> tuple2 = (Tuple2)iterator.next();
                SerializableRange range = ((TopicSubscription)tuple2.f0).getRange() != null ? ((TopicSubscription)tuple2.f0).getRange() : SerializableRange.ofFullRange();
                TopicRange topicRange = new TopicRange(((TopicSubscription)tuple2.f0).getTopic(), range.getPulsarRange());
                this.restoredState.put(topicRange, tuple2.f1);
                String subscriptionName = ((TopicSubscription)tuple2.f0).getSubscriptionName();
                if (!this.stateSubEqualexternalSub && StringUtils.equals(subscriptionName, this.externalSubscriptionName)) {
                    this.stateSubEqualexternalSub = true;
                    logger.info("Source restored state with subscriptionName {}", subscriptionName);
                }
            }

            logger.info("Source subtask {} restored state {}", this.taskIndex, StringUtils.join(new Set[]{this.restoredState.entrySet()}));
        } else {
            logger.info("Source subtask {} has no restore state", this.taskIndex);
        }

    }

    @VisibleForTesting
    static TupleSerializer<Tuple2<TopicSubscription, MessageId>> createStateSerializer() {
        TypeSerializer<?>[] fieldSerializers = new TypeSerializer[]{TopicSubscriptionSerializer.INSTANCE, MessageIdSerializer.INSTANCE};
        TypeInformation<Tuple2<TopicSubscription, MessageId>> info =
                TypeInformation.of(new TypeHint<>() {});
        return new TupleSerializer<>(info.getTypeClass(), fieldSerializers);
    }

    private Iterator<Tuple2<TopicSubscription, MessageId>> tryMigrateState(OperatorStateStore stateStore) throws Exception {
        logger.info("restore old state version {}", this.oldStateVersion);
        PulsarSourceStateSerializer stateSerializer = new PulsarSourceStateSerializer(this.getRuntimeContext().getExecutionConfig());
        ListState<?> rawStates = stateStore.getUnionListState(new ListStateDescriptor("topic-partition-offset-states", stateSerializer.getSerializer(this.oldStateVersion)));
        ListState<String> oldUnionSubscriptionNameStates = stateStore.getUnionListState(new ListStateDescriptor("topic-partition-offset-states_subName", TypeInformation.of(new TypeHint<String>() {
        })));
        Iterator<String> subNameIterator = ((Iterable)oldUnionSubscriptionNameStates.get()).iterator();
        Iterator<?> tuple2s = ((Iterable)rawStates.get()).iterator();
        logger.info("restore old state has data {}", tuple2s.hasNext());
        List<Tuple2<TopicSubscription, MessageId>> records = new ArrayList();

        while(tuple2s.hasNext()) {
            Object next = tuple2s.next();
            Tuple2<TopicSubscription, MessageId> tuple2 = stateSerializer.deserialize(this.oldStateVersion, next);
            String subName = ((TopicSubscription)tuple2.f0).getSubscriptionName();
            if (subNameIterator.hasNext()) {
                subName = (String)subNameIterator.next();
            }

            TopicSubscription topicSubscription = TopicSubscription.builder().topic(((TopicSubscription)tuple2.f0).getTopic()).range(((TopicSubscription)tuple2.f0).getRange()).subscriptionName(subName).build();
            Tuple2<TopicSubscription, MessageId> record = Tuple2.of(topicSubscription, tuple2.f1);
            logger.info("migrationState {}", record);
            records.add(record);
        }

        rawStates.clear();
        oldUnionSubscriptionNameStates.clear();
        return records.listIterator();
    }

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!this.running) {
            logger.debug("snapshotState() called on closed source");
        } else {
            this.unionOffsetStates.clear();
            PulsarFetcher<T> fetcher = this.pulsarFetcher;
            if (fetcher == null) {
                Iterator var3 = this.ownedTopicStarts.entrySet().iterator();

                while(var3.hasNext()) {
                    Map.Entry<TopicRange, MessageId> entry = (Map.Entry)var3.next();
                    TopicSubscription topicSubscription = TopicSubscription.builder().topic(((TopicRange)entry.getKey()).getTopic()).range(((TopicRange)entry.getKey()).getRange()).subscriptionName(this.getSubscriptionName()).build();
                    this.unionOffsetStates.add(Tuple2.of(topicSubscription, entry.getValue()));
                }

                this.pendingOffsetsToCommit.put(context.getCheckpointId(), this.restoredState);
            } else {
                Map<TopicRange, MessageId> currentOffsets = fetcher.snapshotCurrentState();
                this.pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
                Iterator var8 = currentOffsets.entrySet().iterator();

                while(var8.hasNext()) {
                    Map.Entry<TopicRange, MessageId> entry = (Map.Entry)var8.next();
                    logger.warn(String.format("snapshotState %d, %s, %s", context.getCheckpointId(), entry.getKey().toString(), entry.getValue().toString()));
                    TopicSubscription topicSubscription = TopicSubscription.builder().topic(((TopicRange)entry.getKey()).getTopic()).range(((TopicRange)entry.getKey()).getRange()).subscriptionName(this.getSubscriptionName()).build();
                    this.unionOffsetStates.add(Tuple2.of(topicSubscription, entry.getValue()));
                }

                int exceed = this.pendingOffsetsToCommit.size() - 100;
                Iterator<Long> iterator = this.pendingOffsetsToCommit.keySet().iterator();

                while(iterator.hasNext() && exceed > 0) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }

    }

    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!this.running) {
            logger.info("notifyCheckpointComplete() called on closed source");
        } else {
            PulsarFetcher<T> fetcher = this.pulsarFetcher;
            if (fetcher == null) {
                logger.info("notifyCheckpointComplete() called on uninitialized source");
            } else {
                logger.debug("Source {} received confirmation for unknown checkpoint id {}", this.taskIndex, checkpointId);

                try {
                    if (!this.pendingOffsetsToCommit.containsKey(checkpointId)) {
                        logger.warn("Source {} received confirmation for unknown checkpoint id {}", this.taskIndex, checkpointId);
                        return;
                    }

                    Map<TopicRange, MessageId> offset = (Map)this.pendingOffsetsToCommit.get(checkpointId);
                    Iterator<Long> iterator = this.pendingOffsetsToCommit.keySet().iterator();

                    while(iterator.hasNext()) {
                        Long key = (Long)iterator.next();
                        iterator.remove();
                        if (Objects.equals(key, checkpointId)) {
                            break;
                        }
                    }

                    if (offset == null || offset.size() == 0) {
                        logger.debug("Source {} has empty checkpoint state", this.taskIndex);
                        return;
                    }

                    fetcher.commitOffsetToPulsar(offset, this.offsetCommitCallback);
                } catch (Exception var7) {
                    if (this.running) {
                        throw var7;
                    }
                }

            }
        }
    }

    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        logger.error("checkpoint aborted, checkpointId: {}", checkpointId);
    }

    public Map<TopicRange, MessageId> offsetForEachTopic(Set<TopicRange> topics, StartupMode mode, Map<TopicRange, MessageId> specificStartupOffsets) {
        switch (mode) {
            case LATEST:
            case TIMESTAMP:
                return (Map)topics.stream().collect(Collectors.toMap((k) -> {
                    return k;
                }, (k) -> {
                    return MessageId.latest;
                }));
            case EARLIEST:
                return (Map)topics.stream().collect(Collectors.toMap((k) -> {
                    return k;
                }, (k) -> {
                    return MessageId.earliest;
                }));
            case SPECIFIC_OFFSETS:
                Preconditions.checkArgument(topics.containsAll(specificStartupOffsets.keySet()), String.format("Topics designated in startingOffsets should appear in %s, topics:%s, topics in offsets: %s", StringUtils.join(new Set[]{PulsarOptions.TOPIC_OPTION_KEYS}), StringUtils.join(topics.toArray()), StringUtils.join(specificStartupOffsets.entrySet().toArray())));
                Map<TopicRange, MessageId> specificOffsets = new HashMap();
                Iterator var8 = topics.iterator();

                while(var8.hasNext()) {
                    TopicRange topic = (TopicRange)var8.next();
                    if (specificStartupOffsets.containsKey(topic)) {
                        specificOffsets.put(topic, specificStartupOffsets.get(topic));
                    } else {
                        specificOffsets.put(topic, MessageId.latest);
                    }
                }

                return specificOffsets;
            case EXTERNAL_SUBSCRIPTION:
                Map<TopicRange, MessageId> offsetsFromSubs = new HashMap();
                Iterator var6 = topics.iterator();

                while(var6.hasNext()) {
                    TopicRange topic = (TopicRange)var6.next();
                    offsetsFromSubs.put(topic, this.metadataReader.getPositionFromSubscription(topic, this.subscriptionPosition));
                }

                return offsetsFromSubs;
            default:
                return null;
        }
    }

    public Map<Long, Map<TopicRange, MessageId>> getPendingOffsetsToCommit() {
        return this.pendingOffsetsToCommit;
    }

    public Map<TopicRange, MessageId> getOwnedTopicStarts() {
        return this.ownedTopicStarts;
    }
}