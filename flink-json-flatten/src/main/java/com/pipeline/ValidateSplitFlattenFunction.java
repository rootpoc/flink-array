package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import com.pipeline.common.PaginationSchema;
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

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Merged Flink operator that validates, splits, and flattens incoming JSON messages
 * in a single pass — replacing the former {@code ArraySplitterFunction} +
 * {@code FlatteningDeserializer} chain.
 *
 * <h2>Single parse, zero intermediate serialisation</h2>
 * The former two-operator design parsed the input JSON once in the splitter,
 * serialised each page back to {@code byte[]}, then immediately re-parsed those bytes
 * in the flattener. This operator eliminates that round-trip: the input JSON is parsed
 * exactly once and each page slice is flattened directly into a named Flink {@link Row}.
 *
 * <h2>Processing steps per message</h2>
 * <ol>
 *   <li>Parse {@code byte[]} → {@link JsonNode} (<b>once</b>).</li>
 *   <li>Validate required top-level fields against the input schema → DLQ on failure.</li>
 *   <li>Validate required fields in every array item → DLQ on failure.</li>
 *   <li>Split the input array into pages of {@code pageSize} items.</li>
 *   <li>For each page, write pagination metadata and flatten array items directly
 *       into a named {@link Row} — no intermediate {@code byte[]} or {@code ObjectNode}
 *       page is constructed.</li>
 * </ol>
 *
 * <h2>Conversion count compared to former chain</h2>
 * <pre>
 *   Former:  byte[] → JsonNode → byte[] (page) → JsonNode → Row   (4 conversions)
 *   Now:     byte[] → JsonNode → Row                               (2 conversions)
 * </pre>
 */
public final class ValidateSplitFlattenFunction
        extends ProcessFunction<ProcessedMessage, ProcessedMessage>
        implements ResultTypeQueryable<ProcessedMessage> {

    private static final long serialVersionUID = 1L;


    private static final Logger LOG = LoggerFactory.getLogger(ValidateSplitFlattenFunction.class);

    /** Side-output tag for messages that fail validation or cannot be parsed. */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<DlqRecord>("dlq-validate-split-flatten") {};

    // ── Config ────────────────────────────────────────────────────────────────

    private final InputSchemaInfo  inputSchema;
    private final PaginationSchema outputSchema;
    private final int              pageSize;
    private final NullHandling     nullHandling;

    // ── Per-thread state ───────────────────────────────────────────────────────

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    // ── Constructor ───────────────────────────────────────────────────────────

    public ValidateSplitFlattenFunction(
            InputSchemaInfo  inputSchema,
            PaginationSchema outputSchema,
            int              pageSize,
            NullHandling     nullHandling) {
        this.inputSchema  = inputSchema;
        this.outputSchema = outputSchema;
        this.pageSize     = pageSize;
        this.nullHandling = nullHandling;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration params) {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
        LOG.info("ValidateSplitFlattenFunction opened: inputSchema={}, pageSize={}, nullHandling={}",
                inputSchema, pageSize, nullHandling);
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

            // Step 2+3 — validate against input schema
            List<String> violations = validate(root);
            if (!violations.isEmpty()) {
                LOG.warn("Input validation failed ({} bytes): {}", bytes.length, violations);
                ctx.output(DLQ_TAG, DlqRecord.of(msg,
                        new IllegalArgumentException("Input schema validation failed: " + violations)));
                return;
            }

            if (inputSchema.getArrayFieldName() == null) {
                // ── Flat schema: no array field — flatten the whole message into one Row ──
                Row row = Row.withNames();
                flattenInto(root, "", row);
                out.collect(msg.withPayload(row));
            } else {
                // ── Array schema: split array into pages, flatten each page ──────────────

                // Step 4 — locate array and compute pagination
                JsonNode arrayNode = root.path(inputSchema.getArrayFieldName());
                int arraySize  = arrayNode.size();
                int totalPages = (int) Math.ceil((double) arraySize / pageSize);

                // Step 5 — emit one Row per page, flattened directly (no intermediate byte[])
                String arrayOutField = outputSchema.getArrayFieldName();
                for (int p = 0; p < totalPages; p++) {
                    int start = p * pageSize;
                    int end   = Math.min(start + pageSize, arraySize);
                    int count = end - start;

                    // Validate required item fields for every item in this page.
                    // Invalid pages are routed to DLQ individually; other pages continue.
                    if (!inputSchema.getRequiredItemFields().isEmpty()) {
                        List<String> pageViolations = new ArrayList<>();
                        for (int i = start; i < end; i++) {
                            JsonNode item = arrayNode.get(i);
                            for (String path : inputSchema.getRequiredItemFields()) {
                                if (isMissingRequired(item, path)) {
                                    pageViolations.add("missing required field '" + path + "' in "
                                            + inputSchema.getArrayFieldName() + "[" + i + "]");
                                }
                            }
                        }
                        if (!pageViolations.isEmpty()) {
                            LOG.warn("Item validation failed for page {} ({} bytes): {}",
                                    p, bytes.length, pageViolations);
                            ctx.output(DLQ_TAG, DlqRecord.of(msg,
                                    new IllegalArgumentException(
                                            "Item validation failed: " + pageViolations)));
                            continue;
                        }
                    }

                    Row row = Row.withNames();

                    // Pagination metadata
                    row.setField(outputSchema.getIndexFieldName(), p);
                    row.setField(outputSchema.getTotalFieldName(), totalPages);
                    row.setField(outputSchema.getCountFieldName(), count);

                    // Flatten each item in the slice directly into the Row
                    for (int i = start; i < end; i++) {
                        flattenInto(arrayNode.get(i), arrayOutField + "." + (i - start), row);
                    }

                    out.collect(msg.withPayload(row));
                }
            }

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

    /**
     * Returns {@code true} if the required field at {@code dotPath} is absent from {@code node}.
     *
     * <p>For a nested path like {@code "address.street"}, the check only applies when every
     * ancestor is present — if {@code address} itself is missing the condition is not applicable
     * and {@code false} is returned (consistent with JSON Schema semantics).
     */
    private static boolean isMissingRequired(JsonNode node, String dotPath) {
        String[] parts = dotPath.split("\\.");
        JsonNode current = node;
        for (int i = 0; i < parts.length - 1; i++) {
            current = current.path(parts[i]);
            if (current.isMissingNode()) return false;  // parent absent — not applicable
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
