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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Converts a Flink {@link Row} (named-field mode) into a CSV line and produces
 * a Kafka {@link ProducerRecord} for the output topic.
 *
 * <h2>Column contract</h2>
 * Column names and their order are loaded once from a JSON Schema file via
 * {@link SchemaAnalyzer#loadSchema} + {@link SchemaAnalyzer#extractCsvColumns}.
 * The schema's {@code "required"} array defines every column — all fields are
 * mandatory in CSV.  Fields absent from the Row are written as empty strings.
 *
 * <h2>RFC 4180 encoding</h2>
 * A field value is quoted with double-quotes whenever it contains a comma,
 * a double-quote character, a newline ({@code \n}), or a carriage-return
 * ({@code \r}).  A literal {@code "} inside a quoted field is escaped as
 * {@code ""}.
 *
 * <h2>Example</h2>
 * <pre>
 *   Schema required: ["firstName", "lastName", "age", "address.street"]
 *
 *   Row:
 *     firstName      → "Alice"
 *     lastName       → "Smith, Jr."   (contains comma → quoted)
 *     age            → 30
 *     address.street → "5th Ave"
 *
 *   CSV output:
 *     Alice,"Smith, Jr.",30,5th Ave
 * </pre>
 *
 * <h2>Serialization safety</h2>
 * The {@link List} of column names is a plain {@code List<String>} stored in the
 * object — serialized with the Flink operator graph, no file I/O at worker start-up.
 */
public final class CsvSerializer
        implements KafkaRecordSerializationSchema<ProcessedMessage>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvSerializer.class);

    private final String       outputTopic;
    private final List<String> columns;

    /**
     * @param outputTopic Kafka topic name for the output records
     * @param schemaPath  classpath resource path of the CSV JSON Schema
     *                    (e.g. {@code "schemas/csv-person-flat.schema.json"})
     * @throws IOException if the schema cannot be loaded or is invalid
     */
    public CsvSerializer(String outputTopic, String schemaPath) throws IOException {
        this.outputTopic = outputTopic;
        this.columns     = SchemaAnalyzer.extractCsvColumns(SchemaAnalyzer.loadSchema(schemaPath));
        LOG.info("CsvSerializer: topic='{}', columns={}", outputTopic, columns);
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            ProcessedMessage msg, KafkaSinkContext ctx, Long timestamp) {
        try {
            byte[] csvBytes = toCsvLine(msg.getPayload()).getBytes(StandardCharsets.UTF_8);

            RecordHeaders outHeaders = new RecordHeaders();
            for (Map.Entry<String, byte[]> h : msg.getHeaders().entrySet()) {
                outHeaders.add(h.getKey(), h.getValue());
            }
            return new ProducerRecord<>(outputTopic, null, timestamp,
                    msg.getKafkaKey(), csvBytes, outHeaders);

        } catch (Exception e) {
            LOG.error("Failed to serialize Row to CSV: {}", e.getMessage(), e);
            return null;
        }
    }

    // ── CSV line builder ──────────────────────────────────────────────────────

    /**
     * Converts a {@link Row} to a single CSV line using the column order from the schema.
     * Fields absent in the Row are written as empty strings.
     */
    public String toCsvLine(Row row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(',');
            Object value = (row != null) ? row.getField(columns.get(i)) : null;
            sb.append(encodeCsvField(value));
        }
        return sb.toString();
    }

    /**
     * Encodes a single field value following RFC 4180:
     * <ul>
     *   <li>{@code null} → empty string</li>
     *   <li>Values containing {@code ,}, {@code "}, {@code \n}, or {@code \r}
     *       → wrapped in double-quotes; internal {@code "} doubled to {@code ""}</li>
     *   <li>All other values → {@link Object#toString()} as-is</li>
     * </ul>
     */
    static String encodeCsvField(Object value) {
        if (value == null) return "";
        String s = value.toString();
        if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0
                || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0) {
            return '"' + s.replace("\"", "\"\"") + '"';
        }
        return s;
    }

    /** Returns the ordered column list loaded from the schema (useful for testing). */
    public List<String> getColumns() {
        return columns;
    }
}
