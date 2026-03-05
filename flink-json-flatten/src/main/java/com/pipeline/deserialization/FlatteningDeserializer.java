package com.pipeline.deserialization;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.typeinfo.FlatRowTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Flink {@link ProcessFunction} that converts raw Kafka JSON bytes into a Flink
 * {@link Row} (named-field mode via {@link Row#withNames()}).
 *
 * <h2>Flattening rules</h2>
 * <ul>
 *   <li>JSON objects → dot-separated path:  {@code person.address.city}</li>
 *   <li>JSON arrays  → dot + zero-based index: {@code person.tags.0}, {@code person.tags.1}</li>
 *   <li>Leaf values  → typed Java object (String, Integer, Long, Double, Float,
 *                       Boolean, BigDecimal, null) stored via {@link Row#setField(String, Object)}</li>
 *   <li>Nesting depth and array size are unbounded — all levels of the incoming
 *                       message are flattened as-is.</li>
 * </ul>
 *
 * <h2>Example</h2>
 * <pre>
 *   Input:  {"person":{"name":"alice","details":[{"street":"A"},{"street":"B"}]}}
 *
 *   Output Row (named):
 *     row.getField("person.name")             → "alice"
 *     row.getField("person.details.0.street") → "A"
 *     row.getField("person.details.1.street") → "B"
 * </pre>
 *
 * <h2>Error handling</h2>
 * Only genuine parse failures (malformed JSON, null/empty input) are routed to
 * {@link #DLQ_TAG} via side-output. The main stream never receives failed records;
 * the job does not fail.
 *
 * <h2>Performance</h2>
 * <ul>
 *   <li>{@link ObjectMapper} reused per-thread via {@link ThreadLocal} — zero GC allocation.</li>
 *   <li>Flattening is iterative (explicit {@link Deque}), never recursive — safe regardless
 *       of nesting depth or array size.</li>
 *   <li>{@link Row#withNames()} internally uses a {@link java.util.LinkedHashMap}, so
 *       insertion order is preserved with O(1) named access.</li>
 * </ul>
 */
public final class FlatteningDeserializer
        extends ProcessFunction<byte[], Row>
        implements ResultTypeQueryable<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlatteningDeserializer.class);

    /** Side-output tag for records that could not be parsed (malformed JSON, empty input). */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<DlqRecord>("dlq-flatten") {};

    // ── Config ────────────────────────────────────────────────────────────────

    private final NullHandling nullHandling;

    // ── Reusable per-thread state (transient — not serialized with the operator) ──

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    // ── Constructor ───────────────────────────────────────────────────────────

    public FlatteningDeserializer(PipelineConfig config) {
        this.nullHandling = config.getNullHandling();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        mapperLocal = ThreadLocal.withInitial(() -> {
            ObjectMapper m = new ObjectMapper();
            m.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            m.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
            return m;
        });
        LOG.info("FlatteningDeserializer opened: nullHandling={}", nullHandling);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void processElement(byte[] bytes, Context ctx, Collector<Row> out) {
        if (bytes == null || bytes.length == 0) {
            ctx.output(DLQ_TAG, DlqRecord.of(
                    bytes != null ? bytes : new byte[0],
                    new IllegalArgumentException("Empty or null Kafka message")));
            return;
        }

        try {
            JsonNode root = mapperLocal.get().readTree(bytes);
            Row row = Row.withNames();
            flattenIterative(root, row);
            out.collect(row);

        } catch (Exception e) {
            LOG.warn("Failed to parse/flatten JSON ({} bytes): {}", bytes.length, e.getMessage());
            ctx.output(DLQ_TAG, DlqRecord.of(bytes, e));
        }
    }

    // ── Iterative flattener ───────────────────────────────────────────────────

    /**
     * Flattens the entire {@link JsonNode} tree into the given {@link Row} using an
     * explicit {@link Deque} — no recursion, no depth or size limits.
     *
     * <p>Children are pushed in reverse order so they are popped (processed) in
     * original left-to-right document order.
     *
     * <p>Each leaf value is written directly into the Row via
     * {@link Row#setField(String, Object)}.
     */
    private void flattenIterative(JsonNode root, Row row) {
        // Stack frame: Object[]{String prefix, JsonNode node}
        Deque<Object[]> stack = new ArrayDeque<>(64);
        stack.push(new Object[]{"", root});

        while (!stack.isEmpty()) {
            Object[] frame  = stack.pop();
            String   prefix = (String)   frame[0];
            JsonNode node   = (JsonNode) frame[1];

            if (node.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                List<Object[]> children = new ArrayList<>();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String childKey = prefix.isEmpty()
                            ? entry.getKey()
                            : prefix + "." + entry.getKey();
                    children.add(new Object[]{childKey, entry.getValue()});
                }
                // Reverse-push to preserve original document order on pop
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }

            } else if (node.isArray()) {
                for (int i = node.size() - 1; i >= 0; i--) {
                    String childKey = prefix.isEmpty()
                            ? String.valueOf(i)
                            : prefix + "." + i;
                    stack.push(new Object[]{childKey, node.get(i)});
                }

            } else {
                // Leaf node — write directly into the Row
                Object value = extractLeafValue(node);

                if (value == null) {
                    if (nullHandling == NullHandling.EXCLUDE) {
                        continue;                 // skip this field entirely
                    } else if (nullHandling == NullHandling.REPLACE_EMPTY_STRING) {
                        value = "";
                    }
                    // NullHandling.INCLUDE: keep null as-is, fall through to setField
                }

                row.setField(prefix, value);
            }
        }
    }

    // ── Value extraction ──────────────────────────────────────────────────────

    /**
     * Maps a Jackson leaf node to the most specific Java type:
     * <ul>
     *   <li>Integral numbers fitting {@code int}  → {@link Integer}</li>
     *   <li>Integral numbers fitting {@code long} → {@link Long}</li>
     *   <li>Decimal (BigDecimal precision)        → {@link BigDecimal}</li>
     *   <li>Floating point                        → {@link Double}</li>
     *   <li>Boolean                               → {@link Boolean}</li>
     *   <li>Null / missing                        → {@code null}</li>
     *   <li>Text                                  → {@link String}</li>
     * </ul>
     */
    private static Object extractLeafValue(JsonNode node) {
        if (node.isNull() || node.isMissingNode()) return null;

        if (node.isIntegralNumber()) {
            long lv = node.longValue();
            return (lv >= Integer.MIN_VALUE && lv <= Integer.MAX_VALUE) ? (int) lv : lv;
        }
        if (node.isFloatingPointNumber()) {
            return node.isBigDecimal() ? node.decimalValue() : node.doubleValue();
        }
        if (node.isBoolean()) return node.booleanValue();

        return node.asText();
    }

    // ── ResultTypeQueryable ───────────────────────────────────────────────────

    @Override
    public TypeInformation<Row> getProducedType() {
        return FlatRowTypeInfo.INSTANCE;
    }
}
