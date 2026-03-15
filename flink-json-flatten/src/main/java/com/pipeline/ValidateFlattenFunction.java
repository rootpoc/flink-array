package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Flink operator that validates and flattens an incoming JSON message into a
 * single named Flink {@link Row} — without performing any array splitting.
 *
 * <h2>Processing steps per message</h2>
 * <ol>
 *   <li>Parse {@code byte[]} → {@link JsonNode} (once).</li>
 *   <li>Validate required top-level fields against the input schema → DLQ on failure.</li>
 *   <li>Flatten the entire JSON tree (including all arrays) into a named {@link Row}
 *       using dot-notation keys (e.g. {@code "search_engines.0.imdb.director"}).</li>
 * </ol>
 *
 * <p>The full unsplit Row is emitted downstream so that subsequent operators
 * (e.g. {@link com.pipeline.processing.UpperCaseMapFunction}) can operate on
 * <em>all</em> array elements at once before optional page splitting occurs.
 *
 * <p>Split failures and validation errors are emitted to {@link #DLQ_TAG}.
 */
public final class ValidateFlattenFunction
        extends ProcessFunction<ProcessedMessage, ProcessedMessage>
        implements ResultTypeQueryable<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ValidateFlattenFunction.class);

    /** Side-output tag for messages that fail validation or cannot be parsed. */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<DlqRecord>("dlq-validate-flatten") {};

    // ── Config ────────────────────────────────────────────────────────────────

    private final InputSchemaInfo inputSchema;
    private final NullHandling    nullHandling;

    // ── Per-thread state ──────────────────────────────────────────────────────

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    // ── Constructor ───────────────────────────────────────────────────────────

    public ValidateFlattenFunction(InputSchemaInfo inputSchema, NullHandling nullHandling) {
        this.inputSchema  = inputSchema;
        this.nullHandling = nullHandling;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration params) {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
        LOG.info("ValidateFlattenFunction opened: inputSchema={}, nullHandling={}",
                inputSchema, nullHandling);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void processElement(ProcessedMessage msg, Context ctx, Collector<ProcessedMessage> out) {
        byte[] bytes = msg.getOriginalBytes();
        if (bytes == null || bytes.length == 0) {
            ctx.output(DLQ_TAG, DlqRecord.of(msg,
                    new IllegalArgumentException("Empty or null message")));
            return;
        }

        try {
            // Step 1 — parse once
            JsonNode root = mapperLocal.get().readTree(bytes);

            // Step 2 — validate required fields
            List<String> violations = validate(root);
            if (!violations.isEmpty()) {
                LOG.warn("Input validation failed ({} bytes): {}", bytes.length, violations);
                ctx.output(DLQ_TAG, DlqRecord.of(msg,
                        new IllegalArgumentException("Input schema validation failed: " + violations)));
                return;
            }

            // Step 3 — flatten the entire JSON tree into one Row (all arrays kept intact)
            Row row = Row.withNames();
            flattenInto(root, "", row);
            out.collect(msg.withPayload(row));

        } catch (Exception e) {
            LOG.warn("Failed to process message ({} bytes): {}", bytes.length, e.getMessage());
            ctx.output(DLQ_TAG, DlqRecord.of(msg, e));
        }
    }

    // ── Validation ────────────────────────────────────────────────────────────

    List<String> validate(JsonNode root) {
        List<String> violations = new ArrayList<>();
        for (String path : inputSchema.getRequiredFields()) {
            if (isMissingRequired(root, path)) {
                violations.add("missing required field '" + path + "'");
            }
        }
        return violations;
    }

    private static boolean isMissingRequired(JsonNode node, String dotPath) {
        String[] parts = dotPath.split("\\.");
        JsonNode current = node;
        for (int i = 0; i < parts.length - 1; i++) {
            current = current.path(parts[i]);
            if (current.isMissingNode()) return false;
        }
        return current.path(parts[parts.length - 1]).isMissingNode();
    }

    // ── Iterative flattener ───────────────────────────────────────────────────

    private void flattenInto(JsonNode node, String prefix, Row row) {
        Deque<Object[]> stack = new ArrayDeque<>(32);
        stack.push(new Object[]{prefix, node});

        while (!stack.isEmpty()) {
            Object[] frame = stack.pop();
            String   p     = (String)   frame[0];
            JsonNode n     = (JsonNode) frame[1];

            if (n.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = n.fields();
                List<Object[]> children = new ArrayList<>();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> e = fields.next();
                    String childKey = p.isEmpty() ? e.getKey() : p + "." + e.getKey();
                    children.add(new Object[]{childKey, e.getValue()});
                }
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }

            } else if (n.isArray()) {
                for (int i = n.size() - 1; i >= 0; i--) {
                    String childKey = p.isEmpty() ? String.valueOf(i) : p + "." + i;
                    stack.push(new Object[]{childKey, n.get(i)});
                }

            } else {
                Object value = extractLeafValue(n);
                if (value == null) {
                    if (nullHandling == NullHandling.EXCLUDE) continue;
                    if (nullHandling == NullHandling.REPLACE_EMPTY_STRING) value = "";
                }
                row.setField(p, value);
            }
        }
    }

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
    public TypeInformation<ProcessedMessage> getProducedType() {
        return ProcessedMessageTypeInfo.INSTANCE;
    }
}

