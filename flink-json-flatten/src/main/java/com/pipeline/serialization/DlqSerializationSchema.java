package com.pipeline.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.DlqRecord;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Serializes {@link DlqRecord} to a Kafka {@link ProducerRecord} on the DLQ topic.
 *
 * <h2>Output JSON format</h2>
 * <pre>
 * {
 *   "errorClass":       "com.fasterxml.jackson.core.JsonParseException",
 *   "errorMessage":     "Unexpected character ...",
 *   "errorTimestampMs": 1700000000000,
 *   "sourceTopic":      "input-topic",
 *   "sourcePartition":  3,
 *   "sourceOffset":     12345,
 *   "originalBytes":    "base64-encoded-original-message"
 * }
 * </pre>
 *
 * <p>The original bytes are Base64-encoded so the DLQ message is valid JSON
 * even if the source was binary or malformed UTF-8.
 */
public final class DlqSerializationSchema
        implements KafkaRecordSerializationSchema<DlqRecord> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DlqSerializationSchema.class);

    private final String dlqTopic;

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    public DlqSerializationSchema(String dlqTopic) {
        this.dlqTopic = dlqTopic;
    }

    @Override
    public void open(
            org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext context,
            KafkaSinkContext sinkContext) {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DlqRecord record, KafkaSinkContext context, Long timestamp) {
        try {
            ObjectMapper mapper = mapperLocal.get();
            ObjectNode node = mapper.createObjectNode();

            node.put("errorClass",       record.getErrorClass());
            node.put("errorMessage",     record.getErrorMessage());
            node.put("errorTimestampMs", record.getErrorTimestampMs());
            node.put("sourceTopic",      record.getSourceTopic());
            node.put("sourcePartition",  record.getSourcePartition());
            node.put("sourceOffset",     record.getSourceOffset());

            if (record.getOriginalBytes() != null) {
                node.put("originalBytes",
                        Base64.getEncoder().encodeToString(record.getOriginalBytes()));
                // Also try to include as UTF-8 text for readability (best-effort)
                try {
                    node.put("originalText",
                            new String(record.getOriginalBytes(), StandardCharsets.UTF_8));
                } catch (Exception ignored) {
                    // Binary payload — skip text representation
                }
            }

            byte[] value = mapper.writeValueAsBytes(node);
            return new ProducerRecord<>(dlqTopic, null, record.getErrorTimestampMs(), null, value);

        } catch (Exception e) {
            LOG.error("Failed to serialize DlqRecord — dropping: {}", e.getMessage(), e);
            return null;
        }
    }
}
