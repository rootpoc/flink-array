package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.SerializedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import com.pipeline.config.PipelineConfig;
import com.pipeline.deserialization.KafkaEnvelopeDeserializer;
import com.pipeline.processing.UpperCaseMapFunction;
import com.pipeline.serialization.DlqSerializationSchema;
import com.pipeline.serialization.SerializedMessageKafkaRecordSerializationSchema;
import com.pipeline.splitting.SplitFunction;
import com.pipeline.splitting.schema.SchemaAnalyzer;
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
 * Entry point for the Flink flatten/transform pipeline.
 *
 * <h2>Topology</h2>
 * <pre>
 *  KafkaSource (input-topic, byte[])
 *       │
 *       ▼
 *  InputProcessFunctionFactory.create(config)   ──side-output──► DLQ KafkaSink
 *  Selects JSON or CSV input ProcessFunction and attaches Row payload to ProcessedMessage
 *       │
 *       ▼
 *  UpperCaseMapFunction
 *       │
 *       ▼  (only when processing.split-enabled=true)
 *  SplitFunction
 *       │
 *       ▼
 *  OutputProcessFunctionFactory.create(config)  ──side-output──► DLQ KafkaSink
 *  Selects JSON or CSV output ProcessFunction and serializes Row payload bytes
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

        DataStream<ProcessedMessage> rawStream = env
                .fromSource(
                        buildKafkaSource(config),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "kafka-source")
                .setParallelism(P)
                .uid("kafka-source");

        PaginationSchema outputSchema = null;
        if (config.isSplitEnabled()) {
            try {
                outputSchema = SchemaAnalyzer.analyze(loadSchema(config.getOutputSchemaResource()));
            } catch (Exception e) {
                throw new RuntimeException("Failed to load/analyze output schema: "
                        + config.getOutputSchemaResource(), e);
            }
        }

        SingleOutputStreamOperator<ProcessedMessage> rowStream = rawStream
                .process(InputProcessFunctionFactory.create(config))
                .name("input-process")
                .uid("input-process")
                .setParallelism(P)
                .returns(ProcessedMessageTypeInfo.INSTANCE);

        rowStream.getSideOutput(InputProcessFunctionFactory.DLQ_TAG)
                .sinkTo(KafkaSink.<DlqRecord>builder()
                        .setBootstrapServers(config.getBootstrapServers())
                        .setRecordSerializer(new DlqSerializationSchema(config.getDlqTopic()))
                        .setKafkaProducerConfig(baseProducerProps())
                        .build())
                .name("dlq-sink-input")
                .uid("dlq-sink-input")
                .setParallelism(P);

        DataStream<ProcessedMessage> processedStream = rowStream
                .map(new UpperCaseMapFunction(config))
                .name("uppercase-map")
                .uid("uppercase-map")
                .setParallelism(P)
                .returns(ProcessedMessageTypeInfo.INSTANCE);

        final DataStream<ProcessedMessage> preserializeStream;
        if (config.isSplitEnabled()) {
            LOG.info("Split enabled on field '{}', pageSize={}, inputFormat={}, outputFormat={}, inputSchema='{}', outputSchema='{}'",
                    config.getSplitField(), config.getSplittingPageSize(),
                    config.getInputFormat(), config.getOutputFormat(),
                    config.getInputSchemaResource(), config.getOutputSchemaResource());
            preserializeStream = processedStream
                    .flatMap(new SplitFunction(
                            outputSchema,
                            config.getSplittingPageSize(),
                            config.getSplitField()))
                    .name("split-pages")
                    .uid("split-pages")
                    .setParallelism(P)
                    .returns(ProcessedMessageTypeInfo.INSTANCE);
        } else {
            LOG.info("Split disabled — records flow directly to output serialization");
            preserializeStream = processedStream;
        }

        SingleOutputStreamOperator<SerializedMessage> serializedStream = preserializeStream
                .process(OutputProcessFunctionFactory.create(config))
                .name("output-process")
                .uid("output-process")
                .setParallelism(P)
                .returns(org.apache.flink.api.common.typeinfo.TypeInformation.of(SerializedMessage.class));

        serializedStream.getSideOutput(OutputProcessFunctionFactory.DLQ_TAG)
                .sinkTo(KafkaSink.<DlqRecord>builder()
                        .setBootstrapServers(config.getBootstrapServers())
                        .setRecordSerializer(new DlqSerializationSchema(config.getDlqTopic()))
                        .setKafkaProducerConfig(baseProducerProps())
                        .build())
                .name("dlq-sink-output")
                .uid("dlq-sink-output")
                .setParallelism(P);

        Properties eosProps = baseProducerProps();
        eosProps.setProperty("transaction.timeout.ms", String.valueOf(config.getTransactionTimeoutMs()));

        serializedStream
                .sinkTo(KafkaSink.<SerializedMessage>builder()
                        .setBootstrapServers(config.getBootstrapServers())
                        .setRecordSerializer(new SerializedMessageKafkaRecordSerializationSchema(config.getOutputTopic()))
                        .setTransactionalIdPrefix(config.getTransactionPrefix())
                        .setKafkaProducerConfig(eosProps)
                        .build())
                .name("kafka-sink")
                .uid("kafka-sink")
                .setParallelism(P);

        LOG.info("Pipeline built: parallelism={}, inputFormat={}, outputFormat={}, splitEnabled={}, splitField='{}', {}->{} (dlq={})",
                P, config.getInputFormat(), config.getOutputFormat(), config.isSplitEnabled(),
                config.getSplitField(), config.getInputTopic(), config.getOutputTopic(), config.getDlqTopic());
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
