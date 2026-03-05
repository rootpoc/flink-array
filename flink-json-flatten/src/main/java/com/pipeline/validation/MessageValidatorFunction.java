package com.pipeline.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.DlqRecord;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink {@link ProcessFunction} that validates required fields in a raw JSON message
 * before it enters the splitting / flattening stages.
 *
 * <h2>Two validation layers</h2>
 * <ol>
 *   <li><b>Top-level</b> — every field name in {@code requiredTopLevelFields} must be
 *       present as a direct child of the root JSON object.</li>
 *   <li><b>Array-item</b> — every field name in {@code requiredItemFields} must be
 *       present in <em>each</em> element of the array identified by
 *       {@code inputArrayField}. An absent or non-array field causes no item-level
 *       errors (top-level validation already catches that case).</li>
 * </ol>
 *
 * <p>Any validation failure routes the original bytes to {@link #DLQ_TAG} unchanged.
 * Valid messages are forwarded on the main output unchanged.
 */
public final class MessageValidatorFunction
        extends ProcessFunction<byte[], byte[]>
        implements ResultTypeQueryable<byte[]> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(MessageValidatorFunction.class);

    /** Side-output tag for messages that fail required-field validation. */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<DlqRecord>("dlq-validator") {};

    // ── Configuration ──────────────────────────────────────────────────────────

    private final String       inputArrayField;
    private final List<String> requiredTopLevelFields;
    private final List<String> requiredItemFields;

    // ── Per-thread state ───────────────────────────────────────────────────────

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    // ── Constructor ───────────────────────────────────────────────────────────

    /**
     * @param inputArrayField        name of the array property in the root JSON
     *                               (used to locate items for item-level validation)
     * @param requiredTopLevelFields field names that must exist in the root JSON object
     * @param requiredItemFields     field names that must exist in every array element
     */
    public MessageValidatorFunction(
            String       inputArrayField,
            List<String> requiredTopLevelFields,
            List<String> requiredItemFields) {
        this.inputArrayField        = inputArrayField;
        this.requiredTopLevelFields = List.copyOf(requiredTopLevelFields);
        this.requiredItemFields     = List.copyOf(requiredItemFields);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration params) {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void processElement(byte[] bytes, Context ctx, Collector<byte[]> out) {
        if (bytes == null || bytes.length == 0) {
            ctx.output(DLQ_TAG, DlqRecord.of(
                    bytes != null ? bytes : new byte[0],
                    new IllegalArgumentException("Empty or null message")));
            return;
        }

        try {
            JsonNode root = mapperLocal.get().readTree(bytes);
            List<String> violations = validate(root);

            if (!violations.isEmpty()) {
                String msg = "Required field validation failed: " + violations;
                LOG.warn("{} ({} bytes)", msg, bytes.length);
                ctx.output(DLQ_TAG, DlqRecord.of(bytes, new IllegalArgumentException(msg)));
                return;
            }

            out.collect(bytes);  // pass through unchanged

        } catch (Exception e) {
            ctx.output(DLQ_TAG, DlqRecord.of(bytes, e));
        }
    }

    // ── Validation logic (package-visible for direct unit testing) ────────────

    /**
     * Validates the parsed JSON root against both validation layers.
     *
     * @return list of human-readable violation descriptions; empty means valid
     */
    List<String> validate(JsonNode root) {
        List<String> violations = new ArrayList<>();

        // Layer 1 — top-level required fields
        for (String field : requiredTopLevelFields) {
            if (root.path(field).isMissingNode()) {
                violations.add("missing top-level field '" + field + "'");
            }
        }

        // Layer 2 — per-item required fields in the array
        if (!requiredItemFields.isEmpty()) {
            JsonNode array = root.path(inputArrayField);
            if (array.isArray()) {
                for (int i = 0; i < array.size(); i++) {
                    JsonNode item = array.get(i);
                    for (String field : requiredItemFields) {
                        if (item.path(field).isMissingNode()) {
                            violations.add("missing field '" + field + "' in "
                                    + inputArrayField + "[" + i + "]");
                        }
                    }
                }
            }
        }

        return violations;
    }

    // ── ResultTypeQueryable ───────────────────────────────────────────────────

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}
