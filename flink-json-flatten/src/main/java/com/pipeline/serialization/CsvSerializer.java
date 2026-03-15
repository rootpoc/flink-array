package com.pipeline.serialization;

import com.pipeline.common.ProcessedMessage;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * CSV serializer backed by a JSON schema whose {@code required} array defines
 * the exact output column order.
 */
public final class CsvSerializer implements KafkaRecordSerializationSchema<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvSerializer.class);

    private final String outputTopic;
    private final String schemaPath;
    private final List<String> columns;

    public CsvSerializer(String outputTopic, String schemaPath) throws IOException {
        this.outputTopic = outputTopic;
        this.schemaPath = schemaPath;
        this.columns = SchemaAnalyzer.extractCsvColumns(SchemaAnalyzer.loadSchema(schemaPath));
        LOG.info("CsvSerializer opened: topic='{}', schemaPath='{}', columns={}",
                outputTopic, schemaPath, columns);
    }

    @Override
    public void open(org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext ctx,
                     KafkaSinkContext sinkCtx) {
        // no-op
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            ProcessedMessage msg, KafkaSinkContext ctx, Long timestamp) {
        try {
            Row row = msg.getPayload();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) sb.append(',');
                Object value = row != null ? row.getField(columns.get(i)) : null;
                sb.append(encodeCsvField(value));
            }
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

            RecordHeaders outHeaders = new RecordHeaders();
            for (Map.Entry<String, byte[]> h : msg.getHeaders().entrySet()) {
                outHeaders.add(h.getKey(), h.getValue());
            }

            return new ProducerRecord<>(outputTopic, null, timestamp,
                    msg.getKafkaKey(), bytes, outHeaders);
        } catch (Exception e) {
            LOG.error("Failed to serialize CSV row using schema '{}': {}", schemaPath, e.getMessage(), e);
            return null;
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    public static String encodeCsvField(Object value) {
        if (value == null) return "";
        String s = String.valueOf(value);
        boolean needsQuotes = s.indexOf(',') >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
        if (!needsQuotes) return s;
        return '"' + s.replace("\"", "\"\"") + '"';
    }
}

