package com.pipeline.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.ProcessedMessage;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Converts a Flink {@link Row} (named-field mode) back into a hierarchical JSON
 * {@link ProducerRecord} for the Kafka output topic.
 *
 * <h2>Algorithm</h2>
 * <ol>
 *   <li>Collect all named fields from {@link Row#getFieldNames(boolean)} into a
 *       {@link TreeMap} — lexicographic order guarantees parent paths are visited
 *       before children, and lower array indices before higher ones.</li>
 *   <li>For each dot-notation key, split on {@code '.'} to obtain path segments.</li>
 *   <li>Walk (and lazily create) the Jackson node tree:
 *     <ul>
 *       <li>If the <em>next</em> segment is all-digits → current key holds an
 *           {@link ArrayNode}; pad with nulls up to the required index.</li>
 *       <li>Otherwise → current key holds a child {@link ObjectNode}.</li>
 *       <li>At the last segment → write the typed leaf value.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h2>Example</h2>
 * <pre>
 *   Row fields:
 *     "person.name"             → "ALICE"
 *     "person.details.0.street" → "A"
 *     "person.details.1.street" → "B"
 *
 *   Output JSON:
 *     {"person":{"name":"ALICE","details":[{"street":"A"},{"street":"B"}]}}
 * </pre>
 *
 * <h2>Known limitation</h2>
 * JSON object keys that are purely numeric strings (e.g. {@code {"0":"foo"}}) are
 * indistinguishable from array indices; they will be reconstructed as array elements.
 * This is an inherent trade-off of schemaless dot-notation encoding.
 *
 * <h2>Performance</h2>
 * {@link ObjectMapper} reused via {@link ThreadLocal}; {@link #splitDotPath} avoids
 * regex by scanning the string once.
 */
public final class ReconstructSerializer
        implements KafkaRecordSerializationSchema<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ReconstructSerializer.class);

    private final String outputTopic;

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    public ReconstructSerializer(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(
            org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext ctx,
            KafkaSinkContext sinkCtx) {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
        LOG.info("ReconstructSerializer opened for topic '{}'", outputTopic);
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            ProcessedMessage msg, KafkaSinkContext ctx, Long timestamp) {
        try {
            ObjectMapper mapper = mapperLocal.get();
            ObjectNode   root   = mapper.createObjectNode();

            Row row = msg.getPayload();
            if (row != null) {
                // Collect named fields from the Row into a sorted map.
                // Row.getFieldNames(false) returns the LinkedHashSet of field names
                // (null only for positional rows, never null for Row.withNames()).
                Set<String> fieldNames = row.getFieldNames(false);
                if (fieldNames != null) {
                    // TreeMap gives lexicographic order: parent before child, index 0 before index 1
                    TreeMap<String, Object> sorted = new TreeMap<>();
                    for (String name : fieldNames) {
                        sorted.put(name, row.getField(name));
                    }
                    for (var entry : sorted.entrySet()) {
                        String[] segments = splitDotPath(entry.getKey());
                        setNested(root, segments, 0, entry.getValue(), mapper);
                    }
                }
            }

            byte[] jsonBytes = mapper.writeValueAsBytes(root);

            // Propagate the original Kafka key and headers onto the output record
            RecordHeaders outHeaders = new RecordHeaders();
            for (Map.Entry<String, byte[]> h : msg.getHeaders().entrySet()) {
                outHeaders.add(h.getKey(), h.getValue());
            }
            return new ProducerRecord<>(outputTopic, null, timestamp,
                    msg.getKafkaKey(), jsonBytes, outHeaders);

        } catch (Exception e) {
            LOG.error("Failed to reconstruct JSON from ProcessedMessage: {}", e.getMessage(), e);
            return null;
        }
    }

    // ── Recursive tree construction ───────────────────────────────────────────

    /**
     * Recursively walks/creates the Jackson node tree to place {@code value}
     * at the path described by {@code segments[idx..end]}.
     *
     * @param current  always an {@link ObjectNode} at each recursive entry
     * @param segments full split path (e.g. ["person","details","0","street"])
     * @param idx      current position in {@code segments}
     * @param value    leaf value to write at the terminal segment
     * @param mapper   used to create node instances
     */
    private static void setNested(
            ObjectNode current,
            String[]   segments,
            int        idx,
            Object     value,
            ObjectMapper mapper) {

        String  seg    = segments[idx];
        boolean isLast = (idx == segments.length - 1);

        if (isLast) {
            current.set(seg, toJsonNode(value, mapper));
            return;
        }

        String  nextSeg   = segments[idx + 1];
        boolean nextIsInt = isNumeric(nextSeg);

        if (nextIsInt) {
            // Next segment is an array index → this key owns an ArrayNode
            ArrayNode arr    = getOrCreateArray(current, seg, mapper);
            int       arrIdx = Integer.parseInt(nextSeg);

            // Pad with null placeholders up to the required index
            while (arr.size() <= arrIdx) {
                arr.addNull();
            }

            if (idx + 2 == segments.length) {
                // The numeric index IS the last segment → scalar array element
                // e.g. "tags.0" = "java"  →  arr[0] = "java"
                arr.set(arrIdx, toJsonNode(value, mapper));
            } else {
                // More path remains → the element at arrIdx is an ObjectNode
                if (arr.get(arrIdx).isNull()) {
                    arr.set(arrIdx, mapper.createObjectNode());
                }
                // Skip the numeric segment: it was consumed as the array index
                setNested((ObjectNode) arr.get(arrIdx), segments, idx + 2, value, mapper);
            }

        } else {
            ObjectNode child = getOrCreateObject(current, seg, mapper);
            setNested(child, segments, idx + 1, value, mapper);
        }
    }

    // ── Node helpers ──────────────────────────────────────────────────────────

    private static ObjectNode getOrCreateObject(ObjectNode parent, String key, ObjectMapper mapper) {
        JsonNode existing = parent.get(key);
        if (existing != null && existing.isObject()) return (ObjectNode) existing;
        ObjectNode child = mapper.createObjectNode();
        parent.set(key, child);
        return child;
    }

    private static ArrayNode getOrCreateArray(ObjectNode parent, String key, ObjectMapper mapper) {
        JsonNode existing = parent.get(key);
        if (existing != null && existing.isArray()) return (ArrayNode) existing;
        ArrayNode arr = mapper.createArrayNode();
        parent.set(key, arr);
        return arr;
    }

    private static JsonNode toJsonNode(Object value, ObjectMapper mapper) {
        if (value == null) return mapper.nullNode();
        if (value instanceof String) return mapper.getNodeFactory().textNode((String) value);
        if (value instanceof Boolean) return mapper.getNodeFactory().booleanNode((Boolean) value);
        if (value instanceof Integer) return mapper.getNodeFactory().numberNode((Integer) value);
        if (value instanceof Long) return mapper.getNodeFactory().numberNode((Long) value);
        if (value instanceof Double) return mapper.getNodeFactory().numberNode((Double) value);
        if (value instanceof Float) return mapper.getNodeFactory().numberNode((Float) value);
        if (value instanceof BigDecimal) return mapper.getNodeFactory().numberNode((BigDecimal) value);
        return mapper.getNodeFactory().textNode(value.toString());
    }

    // ── Path utilities ────────────────────────────────────────────────────────

    /**
     * Splits a dot-notation key into segments without regex.
     * {@code "person.details.0.street"} → {@code ["person","details","0","street"]}
     */
    static String[] splitDotPath(String key) {
        int count = 1;
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == '.') count++;
        }
        String[] parts = new String[count];
        int start = 0, idx = 0;
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == '.') {
                parts[idx++] = key.substring(start, i);
                start = i + 1;
            }
        }
        parts[idx] = key.substring(start);
        return parts;
    }

    /** {@code true} if {@code s} is a non-empty, all-digit string. */
    static boolean isNumeric(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }
}
