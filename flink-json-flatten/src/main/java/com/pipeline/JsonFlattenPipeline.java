package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.config.PipelineConfig;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.processing.UpperCaseMapFunction;
import com.pipeline.serialization.DlqSerializationSchema;
import com.pipeline.deserialization.KafkaEnvelopeDeserializer;
import com.pipeline.serialization.ReconstructSerializer;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import com.pipeline.common.PaginationSchema;
import com.pipeline.splitting.SplitFunction;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Entry point for the Flink JSON Flatten Pipeline.
 *
 * <h2>Topology</h2>
 * <pre>
 *  KafkaSource (input-topic, byte[])
 *       │
 *       ▼
 *  ValidateFlattenFunction       ──side-output──►  DLQ KafkaSink (dlq-topic)
 *  1. Validate required fields against input schema
 *  2. Flatten entire JSON → Row.withNames()  (all arrays present, no split)
 *       │
 *       ▼
 *  UpperCaseMapFunction
 *  Applies string transformation to every matching field across the full Row
 *  (all array elements are in-place — transformation is done once, not per page)
 *       │
 *       ▼  (only when processing.split-enabled=true)
 *  SplitFunction  (splits on processing.split-field)
 *  Splits the processed Row into pages; emits one ProcessedMessage per page,
 *  with items re-indexed from 0 within each page.
 *  When processing.split-enabled=false this step is bypassed entirely.
 *       │
 *       ▼
 *  ReconstructSerializer
 *  Row → hierarchical JSON bytes
 *       │
 *       ▼
 *  KafkaSink (output-topic, exactly-once via Kafka transactions)
 * </pre>
 *
 * <h2>Exactly-once guarantee</h2>
 * <ul>
 *   <li>Source: {@code isolation.level=read_committed}</li>
 *   <li>Sink: Kafka transactions ({@code transactionalIdPrefix} + EOS producer)</li>
 *   <li>{@code transaction.timeout.ms} must exceed checkpoint interval + timeout</li>
 * </ul>
 *
 * <h2>CLI usage</h2>
 * <pre>
 *   flink run -c com.pipeline.JsonFlattenPipeline flink-json-flatten-fat.jar \
 *     --kafka.bootstrap-servers broker1:9092 \
 *     --kafka.input-topic input-topic         \
 *     --kafka.output-topic output-topic       \
 *     --kafka.dlq-topic dlq-topic             \
 *     --job.parallelism 8                     \
 *     --checkpoint.interval-ms 60000          \
 *     --checkpoint.storage s3://bucket/cp     \
 *     --processing.split-enabled true         \
 *     --processing.split-field search_engines \
 *     --processing.third-party-jar /opt/flink/lib/vendor-1.0.jar
 * </pre>
 */
public final class JsonFlattenPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFlattenPipeline.class);

    public static void main(String[] args) throws Exception {
        ParameterTool  params = ParameterTool.fromArgs(args);
        PipelineConfig config = PipelineConfig.fromParameterTool(params);

        LOG.info("Starting JsonFlattenPipeline: {}", config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        configureCheckpointing(env, config);
        buildPipeline(env, config);

        env.execute("JsonFlattenPipeline");
    }

    // ── Checkpoint configuration ──────────────────────────────────────────────

    static void configureCheckpointing(StreamExecutionEnvironment env, PipelineConfig config) {
        env.enableCheckpointing(config.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        env.disableOperatorChaining();
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setCheckpointTimeout(config.getCheckpointTimeoutMs());
        cp.setMinPauseBetweenCheckpoints(config.getCheckpointMinPauseMs());
        cp.setMaxConcurrentCheckpoints(1);
        cp.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.NO_EXTERNALIZED_CHECKPOINTS);
        cp.setCheckpointStorage(config.getCheckpointStorage());

        if (config.isUnalignedCheckpoints()) {
            cp.enableUnalignedCheckpoints();
            cp.setAlignedCheckpointTimeout(Duration.ZERO);
        }
    }

    // ── Pipeline topology ─────────────────────────────────────────────────────

    static void buildPipeline(StreamExecutionEnvironment env, PipelineConfig config) {
        final int P = config.getParallelism();

        // ── 1. Kafka Source ────────────────────────────────────────────────────
        DataStream<ProcessedMessage> rawStream = env
                .fromSource(
                        buildKafkaSource(config),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "kafka-source")
                .setParallelism(P)
                .uid("kafka-source");

        // ── 2. Validate + flatten (no split) ──────────────────────────────────
        InputSchemaInfo  inputSchema;
        PaginationSchema outputSchema;
        try {
            inputSchema  = InputSchemaInfo.analyze(
                    loadSchema("schemas/input-netflix-categories.schema.json"));
            outputSchema = SchemaAnalyzer.analyze(
                    loadSchema("schemas/output-persons-paginated.schema.json"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load/analyze schemas", e);
        }

        SingleOutputStreamOperator<ProcessedMessage> flatStream = rawStream
                .process(new ValidateFlattenFunction(
                        inputSchema,
                        config.getNullHandling()))
                .name("validate-flatten")
                .uid("validate-flatten")
                .setParallelism(P)
                .returns(ProcessedMessageTypeInfo.INSTANCE);

        // ── 2a. DLQ side output (validation failures, parse errors) ───────────
        flatStream.getSideOutput(ValidateFlattenFunction.DLQ_TAG)
                .sinkTo(KafkaSink.<DlqRecord>builder()
                        .setBootstrapServers(config.getBootstrapServers())
                        .setRecordSerializer(new DlqSerializationSchema(config.getDlqTopic()))
                        .setKafkaProducerConfig(baseProducerProps())
                        .build())
                .name("dlq-sink")
                .uid("dlq-sink")
                .setParallelism(P);

        // ── 3. Apply transformation to ALL fields in the full unsplit Row ─────
        DataStream<ProcessedMessage> processedStream = flatStream
                .map(new UpperCaseMapFunction(config))
                .name("uppercase-map")
                .uid("uppercase-map")
                .setParallelism(P)
                .returns(ProcessedMessageTypeInfo.INSTANCE);

        // ── 3a. Conditionally split into pages AFTER all transformations ───────
        final DataStream<ProcessedMessage> presinkStream;
        if (config.isSplitEnabled()) {
            LOG.info("Split enabled on field '{}', pageSize={}",
                    config.getSplitField(), config.getSplittingPageSize());
            presinkStream = processedStream
                    .flatMap(new SplitFunction(
                            outputSchema,
                            config.getSplittingPageSize(),
                            config.getSplitField()))
                    .name("split-pages")
                    .uid("split-pages")
                    .setParallelism(P)
                    .returns(ProcessedMessageTypeInfo.INSTANCE);
        } else {
            LOG.info("Split disabled — records flow directly to sink");
            presinkStream = processedStream;
        }

        // ── 4. Reconstruct JSON and write to Kafka (exactly-once) ─────────────
        Properties eosProps = baseProducerProps();
        eosProps.setProperty("transaction.timeout.ms",
                String.valueOf(config.getTransactionTimeoutMs()));

        presinkStream
                .sinkTo(KafkaSink.<ProcessedMessage>builder()
                        .setBootstrapServers(config.getBootstrapServers())
                        .setRecordSerializer(new ReconstructSerializer(config.getOutputTopic()))
                        .setTransactionalIdPrefix(config.getTransactionPrefix())
                        .setKafkaProducerConfig(eosProps)
                        .build())
                .name("kafka-sink")
                .uid("kafka-sink")
                .setParallelism(P);

        LOG.info("Pipeline built: parallelism={}, splitEnabled={}, splitField='{}', {}->{} (dlq={})",
                P, config.isSplitEnabled(), config.getSplitField(),
                config.getInputTopic(), config.getOutputTopic(), config.getDlqTopic());
    }

    // ── Builders ──────────────────────────────────────────────────────────────

    private static KafkaSource<ProcessedMessage> buildKafkaSource(PipelineConfig config) {
        Properties props = new Properties();
        props.setProperty("isolation.level",   "read_committed");
        props.setProperty("max.poll.records",  "500");
        props.setProperty("fetch.max.wait.ms", "500");

        return KafkaSource.<ProcessedMessage>builder()
                .setBootstrapServers(config.getBootstrapServers())
                .setTopics(config.getInputTopic())
                .setGroupId(config.getConsumerGroup())
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaEnvelopeDeserializer())
                .setProperties(props)
                .build();
    }

    private static JsonNode loadSchema(String classpathPath) throws IOException {
        return SchemaAnalyzer.loadSchema(classpathPath);
    }

    private static Properties baseProducerProps() {
        Properties p = new Properties();
        p.setProperty("acks",             "all");
        p.setProperty("retries",          "10");
        p.setProperty("retry.backoff.ms", "200");
        p.setProperty("linger.ms",        "5");
        p.setProperty("batch.size",       "65536");
        p.setProperty("compression.type", "lz4");
        return p;
    }
}
