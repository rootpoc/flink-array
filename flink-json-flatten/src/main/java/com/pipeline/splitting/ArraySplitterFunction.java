package com.pipeline.splitting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.PaginationSchema;
import com.pipeline.splitting.page.PageBuilder;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink {@link ProcessFunction} that splits a large JSON array into smaller paginated pages.
 *
 * <h2>Inputs and outputs</h2>
 * <ul>
 *   <li><b>Input:</b> {@code byte[]} — raw JSON message containing a top-level array field
 *       named by {@code inputArrayField} (e.g. {@code "persons"}).</li>
 *   <li><b>Main output:</b> one {@code byte[]} per page, each a JSON object whose field names
 *       are defined by {@link PaginationSchema} (array, index, total, count).</li>
 *   <li><b>Side output ({@link #DLQ_TAG}):</b> {@link DlqRecord} for messages that cannot
 *       be parsed or that lack the expected array field.</li>
 * </ul>
 *
 * <h2>Pagination</h2>
 * Given an array of {@code N} items and a page size {@code S}:
 * <ul>
 *   <li>Total pages = ⌈N / S⌉  (0-item arrays produce 0 pages)</li>
 *   <li>Page {@code p} contains items {@code [p·S, min((p+1)·S, N))}</li>
 *   <li>Each page carries a 0-based {@code index}, the {@code total} page count,
 *       and the {@code count} of items in that page.</li>
 * </ul>
 *
 * <h2>Schema-agnostic</h2>
 * The output field names are entirely controlled by the supplied {@link PaginationSchema}.
 * Swapping a different schema automatically adapts the output structure without code changes.
 */
public final class ArraySplitterFunction
        extends ProcessFunction<byte[], byte[]>
        implements ResultTypeQueryable<byte[]> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ArraySplitterFunction.class);

    /** Side-output tag for messages that could not be split (parse failures, missing field). */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<DlqRecord>("dlq-splitter") {};

    // ── Configuration ──────────────────────────────────────────────────────────

    /** Name of the JSON field in the input message that contains the array to split. */
    private final String inputArrayField;

    /** Maximum number of items per output page. */
    private final int pageSize;

    /** Field-name mapping derived from the output JSON Schema. */
    private final PaginationSchema outputSchema;

    // ── Per-thread state (not serialized) ─────────────────────────────────────

    private transient ThreadLocal<ObjectMapper> mapperLocal;

    // ── Constructor ───────────────────────────────────────────────────────────

    public ArraySplitterFunction(
            String inputArrayField,
            int pageSize,
            PaginationSchema outputSchema) {
        this.inputArrayField = inputArrayField;
        this.pageSize        = pageSize;
        this.outputSchema    = outputSchema;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration params) {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
        LOG.info("ArraySplitterFunction opened: inputField='{}', pageSize={}, schema={}",
                inputArrayField, pageSize, outputSchema);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void processElement(byte[] bytes, Context ctx, Collector<byte[]> out) {
        // 1. Guard: null/empty → DLQ
        if (bytes == null || bytes.length == 0) {
            ctx.output(DLQ_TAG, DlqRecord.of(
                    bytes != null ? bytes : new byte[0],
                    new IllegalArgumentException("Empty or null message")));
            return;
        }

        try {
            ObjectMapper mapper = mapperLocal.get();

            // 2. Parse root JSON
            JsonNode root = mapper.readTree(bytes);

            // 3. Locate the input array field
            JsonNode arrayNode = root.path(inputArrayField);
            if (!arrayNode.isArray()) {
                ctx.output(DLQ_TAG, DlqRecord.of(bytes, new IllegalArgumentException(
                        "Field '" + inputArrayField + "' is missing or not a JSON array")));
                return;
            }

            // 4. Compute total pages  (0-item arrays → 0 pages)
            int arraySize  = arrayNode.size();
            int totalPages = (int) Math.ceil((double) arraySize / pageSize);

            // 5. Emit one page per slice
            for (int p = 0; p < totalPages; p++) {
                int start = p * pageSize;
                int end   = Math.min(start + pageSize, arraySize);

                ArrayNode slice = mapper.createArrayNode();
                for (int i = start; i < end; i++) {
                    slice.add(arrayNode.get(i));
                }

                ObjectNode page = PageBuilder.build(outputSchema, mapper, slice, p, totalPages);
                out.collect(mapper.writeValueAsBytes(page));
            }

        } catch (Exception e) {
            LOG.warn("Failed to split array ({} bytes): {}", bytes.length, e.getMessage());
            ctx.output(DLQ_TAG, DlqRecord.of(bytes, e));
        }
    }

    // ── ResultTypeQueryable ───────────────────────────────────────────────────

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}
