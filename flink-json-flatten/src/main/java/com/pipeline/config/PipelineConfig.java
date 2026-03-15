package com.pipeline.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.List;

/**
 * Immutable configuration holder for the JSON-flatten Flink pipeline.
 *
 * <p>Loaded from Flink {@link ParameterTool} (command-line args and/or properties file).
 *
 * <p>Example invocation:
 * <pre>
 *   flink run -c com.pipeline.JsonFlattenPipeline flink-json-flatten-fat.jar \
 *     --kafka.bootstrap-servers broker1:9092,broker2:9092 \
 *     --kafka.input-topic input-topic \
 *     --kafka.output-topic output-topic \
 *     --kafka.dlq-topic dlq-topic \
 *     --kafka.consumer-group flink-json-flatten-cg \
 *     --job.parallelism 8 \
 *     --checkpoint.interval-ms 60000 \
 *     --checkpoint.storage s3://my-bucket/flink-checkpoints/json-flatten \
 *     --processing.third-party-jar /opt/flink/lib/vendor-processor-1.0.0.jar
 * </pre>
 */
public final class PipelineConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── Kafka ──────────────────────────────────────────────────────────────────

    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final String dlqTopic;
    private final String consumerGroup;
    private final String transactionPrefix;
    private final long transactionTimeoutMs;

    // ── Job ────────────────────────────────────────────────────────────────────

    private final int parallelism;

    // ── Checkpoint ────────────────────────────────────────────────────────────

    private final long checkpointIntervalMs;
    private final long checkpointTimeoutMs;
    private final long checkpointMinPauseMs;
    private final boolean unalignedCheckpoints;
    private final String checkpointStorage;

    // ── Processing ────────────────────────────────────────────────────────────

    /**
     * Filesystem path to the third-party JAR loaded at runtime via URLClassLoader.
     * Must be accessible from all TaskManager nodes (e.g. mounted volume or HDFS).
     */
    private final String thirdPartyJarPath;

    /**
     * Fully-qualified class name inside the third-party JAR that exposes a
     * {@code String process(String input)} method.
     */
    private final String processorClassName;

    /**
     * Ordered priority list of dot-notation field keys to uppercase.
     * The first key present in the Row is processed; remaining keys are skipped.
     */
    private final List<String> uppercaseFieldKeys;

    // ── Splitting ─────────────────────────────────────────────────────────────

    /** Name of the JSON array field in the raw input message (e.g. {@code "persons"}). */
    private final String splittingInputArrayField;

    /** Maximum number of items per output page. */
    private final int splittingPageSize;

    /**
     * Whether to perform page splitting at all.
     * When {@code false}, the {@code SplitFunction} step is skipped entirely and
     * each fully-processed record flows directly to the sink as a single message.
     */
    private final boolean splitEnabled;

    /**
     * Name of the flat-Row field whose array should be paginated by {@code SplitFunction}.
     * Corresponds to the top-level array key in the flattened Row (e.g. {@code "search_engines"}).
     * Only used when {@code splitEnabled=true}.
     */
    private final String splitField;

    // ── Flatten ───────────────────────────────────────────────────────────────

    /** How to handle null JSON values: INCLUDE (default), EXCLUDE, REPLACE_EMPTY_STRING. */
    private final NullHandling nullHandling;

    // ── Constructor (use builder) ──────────────────────────────────────────────

    private PipelineConfig(Builder b) {
        this.bootstrapServers     = b.bootstrapServers;
        this.inputTopic           = b.inputTopic;
        this.outputTopic          = b.outputTopic;
        this.dlqTopic             = b.dlqTopic;
        this.consumerGroup        = b.consumerGroup;
        this.transactionPrefix    = b.transactionPrefix;
        this.transactionTimeoutMs = b.transactionTimeoutMs;
        this.parallelism          = b.parallelism;
        this.checkpointIntervalMs = b.checkpointIntervalMs;
        this.checkpointTimeoutMs  = b.checkpointTimeoutMs;
        this.checkpointMinPauseMs = b.checkpointMinPauseMs;
        this.unalignedCheckpoints = b.unalignedCheckpoints;
        this.checkpointStorage    = b.checkpointStorage;
        this.thirdPartyJarPath    = b.thirdPartyJarPath;
        this.processorClassName   = b.processorClassName;
        this.uppercaseFieldKeys        = List.copyOf(b.uppercaseFieldKeys);
        this.splittingInputArrayField  = b.splittingInputArrayField;
        this.splittingPageSize         = b.splittingPageSize;
        this.splitEnabled              = b.splitEnabled;
        this.splitField                = b.splitField;
        this.nullHandling              = b.nullHandling;
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    /**
     * Constructs a {@link PipelineConfig} from {@link ParameterTool}.
     * All parameters have sensible defaults for local development.
     */
    public static PipelineConfig fromParameterTool(ParameterTool p) {
        return new Builder()
                .bootstrapServers(p.get("kafka.bootstrap-servers", "localhost:9092"))
                .inputTopic(p.get("kafka.input-topic", "input-topic"))
                .outputTopic(p.get("kafka.output-topic", "output-topic"))
                .dlqTopic(p.get("kafka.dlq-topic", "dlq-topic"))
                .consumerGroup(p.get("kafka.consumer-group", "flink-json-flatten-cg"))
                .transactionPrefix(p.get("kafka.transaction-prefix", "flink-json-flatten"))
                .transactionTimeoutMs(p.getLong("kafka.transaction-timeout-ms", 900_000L))
                .parallelism(p.getInt("job.parallelism", 4))
                .checkpointIntervalMs(p.getLong("checkpoint.interval-ms", 60_000L))
                .checkpointTimeoutMs(p.getLong("checkpoint.timeout-ms", 120_000L))
                .checkpointMinPauseMs(p.getLong("checkpoint.min-pause-ms", 30_000L))
                .unalignedCheckpoints(p.getBoolean("checkpoint.unaligned", true))
                .checkpointStorage(p.get("checkpoint.storage", "file:///tmp/flink-checkpoints/json-flatten"))
                .thirdPartyJarPath(p.get("processing.third-party-jar", ""))
                .processorClassName(p.get("processing.processor-class", "com.vendor.StringProcessor"))
                .uppercaseFieldKeys(parseList(p.get("processing.uppercase-field-keys", "person.name,name")))
                .splittingInputArrayField(p.get("splitting.input-array-field", "persons"))
                .splittingPageSize(p.getInt("splitting.page-size", 100))
                .splitEnabled(p.getBoolean("processing.split-enabled", true))
                .splitField(p.get("processing.split-field", "search_engines"))
                .nullHandling(NullHandling.valueOf(p.get("flatten.null-handling", "INCLUDE")))
                .build();
    }

    private static List<String> parseList(String csv) {
        return List.of(csv.split(","));
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    public String getBootstrapServers()         { return bootstrapServers; }
    public String getInputTopic()               { return inputTopic; }
    public String getOutputTopic()              { return outputTopic; }
    public String getDlqTopic()                 { return dlqTopic; }
    public String getConsumerGroup()            { return consumerGroup; }
    public String getTransactionPrefix()        { return transactionPrefix; }
    public long getTransactionTimeoutMs()       { return transactionTimeoutMs; }
    public int getParallelism()                 { return parallelism; }
    public long getCheckpointIntervalMs()       { return checkpointIntervalMs; }
    public long getCheckpointTimeoutMs()        { return checkpointTimeoutMs; }
    public long getCheckpointMinPauseMs()       { return checkpointMinPauseMs; }
    public boolean isUnalignedCheckpoints()     { return unalignedCheckpoints; }
    public String getCheckpointStorage()        { return checkpointStorage; }
    public String getThirdPartyJarPath()        { return thirdPartyJarPath; }
    public String getProcessorClassName()       { return processorClassName; }
    public List<String> getUppercaseFieldKeys()        { return uppercaseFieldKeys; }
    public String getSplittingInputArrayField()        { return splittingInputArrayField; }
    public int getSplittingPageSize()                  { return splittingPageSize; }
    public boolean isSplitEnabled()                    { return splitEnabled; }
    public String getSplitField()                      { return splitField; }
    public NullHandling getNullHandling()              { return nullHandling; }

    // ── Inner types ───────────────────────────────────────────────────────────

    public enum NullHandling {
        /** Include null-valued keys in the Row. */
        INCLUDE,
        /** Skip null-valued keys entirely. */
        EXCLUDE,
        /** Replace null with the empty string "". */
        REPLACE_EMPTY_STRING
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {
        private String bootstrapServers      = "localhost:9092";
        private String inputTopic            = "input-topic";
        private String outputTopic           = "output-topic";
        private String dlqTopic              = "dlq-topic";
        private String consumerGroup         = "flink-json-flatten-cg";
        private String transactionPrefix     = "flink-json-flatten";
        private long   transactionTimeoutMs  = 900_000L;
        private int    parallelism           = 4;
        private long   checkpointIntervalMs  = 60_000L;
        private long   checkpointTimeoutMs   = 120_000L;
        private long   checkpointMinPauseMs  = 30_000L;
        private boolean unalignedCheckpoints = true;
        private String checkpointStorage     = "file:///tmp/flink-checkpoints/json-flatten";
        private String thirdPartyJarPath     = "";
        private String processorClassName    = "com.vendor.StringProcessor";
        private List<String> uppercaseFieldKeys      = List.of("person.name", "name");
        private String splittingInputArrayField      = "persons";
        private int    splittingPageSize             = 100;
        private boolean splitEnabled                 = true;
        private String  splitField                   = "search_engines";
        private NullHandling nullHandling            = NullHandling.INCLUDE;

        public Builder bootstrapServers(String v)         { this.bootstrapServers = v;     return this; }
        public Builder inputTopic(String v)               { this.inputTopic = v;           return this; }
        public Builder outputTopic(String v)              { this.outputTopic = v;          return this; }
        public Builder dlqTopic(String v)                 { this.dlqTopic = v;             return this; }
        public Builder consumerGroup(String v)            { this.consumerGroup = v;        return this; }
        public Builder transactionPrefix(String v)        { this.transactionPrefix = v;    return this; }
        public Builder transactionTimeoutMs(long v)       { this.transactionTimeoutMs = v; return this; }
        public Builder parallelism(int v)                 { this.parallelism = v;          return this; }
        public Builder checkpointIntervalMs(long v)       { this.checkpointIntervalMs = v; return this; }
        public Builder checkpointTimeoutMs(long v)        { this.checkpointTimeoutMs = v;  return this; }
        public Builder checkpointMinPauseMs(long v)       { this.checkpointMinPauseMs = v; return this; }
        public Builder unalignedCheckpoints(boolean v)    { this.unalignedCheckpoints = v; return this; }
        public Builder checkpointStorage(String v)        { this.checkpointStorage = v;    return this; }
        public Builder thirdPartyJarPath(String v)        { this.thirdPartyJarPath = v;    return this; }
        public Builder processorClassName(String v)       { this.processorClassName = v;   return this; }
        public Builder uppercaseFieldKeys(List<String> v)        { this.uppercaseFieldKeys = v;           return this; }
        public Builder splittingInputArrayField(String v)        { this.splittingInputArrayField = v;     return this; }
        public Builder splittingPageSize(int v)                  { this.splittingPageSize = v;            return this; }
        public Builder splitEnabled(boolean v)                   { this.splitEnabled = v;                 return this; }
        public Builder splitField(String v)                      { this.splitField = v;                   return this; }
        public Builder nullHandling(NullHandling v)              { this.nullHandling = v;                 return this; }

        public PipelineConfig build() {
            return new PipelineConfig(this);
        }
    }

    @Override
    public String toString() {
        return "PipelineConfig{"
                + "inputTopic='" + inputTopic + '\''
                + ", outputTopic='" + outputTopic + '\''
                + ", dlqTopic='" + dlqTopic + '\''
                + ", parallelism=" + parallelism
                + ", checkpointIntervalMs=" + checkpointIntervalMs
                + ", splitEnabled=" + splitEnabled
                + ", splitField='" + splitField + '\''
                + ", nullHandling=" + nullHandling
                + '}';
    }
}
