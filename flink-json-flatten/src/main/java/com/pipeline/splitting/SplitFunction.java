package com.pipeline.splitting;

import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Flink operator that splits a fully-processed (validated, flattened, transformed)
 * {@link ProcessedMessage} into one {@link ProcessedMessage} per page.
 *
 * <h2>Why split happens last</h2>
 * By running after {@code ValidateFlattenFunction} and {@code UpperCaseMapFunction},
 * every page automatically inherits the already-transformed field values — no
 * re-processing is needed per page.
 *
 * <h2>Input Row structure</h2>
 * The incoming Row uses dot-notation keys for the full JSON tree, including
 * all array elements (e.g. {@code "search_engines.0.name"},
 * {@code "search_engines.1.imdb.director"}, ...).
 *
 * <h2>Output Row structure per page</h2>
 * Each emitted Row contains:
 * <ul>
 *   <li>Pagination metadata fields defined by {@link PaginationSchema}
 *       ({@code index}, {@code total}, {@code count}).</li>
 *   <li>Only the {@code splitField.N.*} keys for the items in this page,
 *       re-indexed from 0 within the page.</li>
 *   <li>All non-array-item fields from the original Row
 *       (top-level scalars and nested objects outside {@code splitField}).</li>
 * </ul>
 *
 * <h2>Conditional activation</h2>
 * This operator is only added to the topology when
 * {@code processing.split-enabled=true} (see {@link com.pipeline.config.PipelineConfig}).
 * When split is disabled, records flow directly from {@code UpperCaseMapFunction}
 * to the sink.
 */
public final class SplitFunction
        extends RichFlatMapFunction<ProcessedMessage, ProcessedMessage>
        implements ResultTypeQueryable<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SplitFunction.class);

    // ── Config ────────────────────────────────────────────────────────────────

    private final PaginationSchema outputSchema;
    private final int              pageSize;
    private final String           splitField;

    // ── Constructor ───────────────────────────────────────────────────────────

    /**
     * @param outputSchema  field-name mapping for pagination metadata
     * @param pageSize      maximum number of array items per output page
     * @param splitField    top-level array key to paginate (e.g. {@code "search_engines"})
     */
    public SplitFunction(PaginationSchema outputSchema, int pageSize, String splitField) {
        this.outputSchema = outputSchema;
        this.pageSize     = pageSize;
        this.splitField   = splitField;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) {
        LOG.info("SplitFunction opened: splitField='{}', pageSize={}, schema={}",
                splitField, pageSize, outputSchema);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void flatMap(ProcessedMessage msg, Collector<ProcessedMessage> out) throws Exception {
        Row row = msg.getPayload();
        if (row == null) {
            out.collect(msg);
            return;
        }

        Set<String> allNames = row.getFieldNames(false);
        if (allNames == null) {
            out.collect(msg);
            return;
        }

        // ── Separate prefix "splitField.N" → find max index N ────────────────
        String arrayPrefix = splitField + ".";
        int maxIndex = -1;
        for (String name : allNames) {
            if (!name.startsWith(arrayPrefix)) continue;
            String rest = name.substring(arrayPrefix.length());
            int dotPos = rest.indexOf('.');
            String indexStr = dotPos >= 0 ? rest.substring(0, dotPos) : rest;
            try {
                int idx = Integer.parseInt(indexStr);
                if (idx > maxIndex) maxIndex = idx;
            } catch (NumberFormatException ignored) {
                // non-numeric segment — not an array item key
            }
        }

        if (maxIndex < 0) {
            // No array items found for splitField — pass through as-is
            LOG.warn("SplitFunction: no array items found for field '{}', passing through", splitField);
            out.collect(msg);
            return;
        }

        int arraySize  = maxIndex + 1;
        int totalPages = (int) Math.ceil((double) arraySize / pageSize);

        // ── Collect all non-array-item keys (shared across every page) ────────
        List<String> sharedKeys = new ArrayList<>();
        for (String name : allNames) {
            if (name.startsWith(arrayPrefix)) {
                // Only skip keys that are direct array items of splitField
                String rest = name.substring(arrayPrefix.length());
                int dotPos = rest.indexOf('.');
                String indexStr = dotPos >= 0 ? rest.substring(0, dotPos) : rest;
                try {
                    Integer.parseInt(indexStr);
                    continue; // this is a splitField array item key — skip from shared
                } catch (NumberFormatException ignored) {
                    // non-numeric — treat as shared
                }
            }
            sharedKeys.add(name);
        }

        // ── Emit one Row per page ──────────────────────────────────────────────
        for (int p = 0; p < totalPages; p++) {
            int start = p * pageSize;
            int end   = Math.min(start + pageSize, arraySize);
            int count = end - start;

            Row pageRow = Row.withNames();

            // Pagination metadata
            pageRow.setField(outputSchema.getIndexFieldName(), p);
            pageRow.setField(outputSchema.getTotalFieldName(), totalPages);
            pageRow.setField(outputSchema.getCountFieldName(), count);

            // Copy shared (non-array-item) fields
            for (String key : sharedKeys) {
                pageRow.setField(key, row.getField(key));
            }

            // Copy this page's array items, re-indexed from 0 within the page
            for (int i = start; i < end; i++) {
                int pageLocalIndex = i - start;
                String srcPrefix  = arrayPrefix + i + ".";
                String dstPrefix  = arrayPrefix + pageLocalIndex + ".";

                for (String name : allNames) {
                    if (name.startsWith(srcPrefix)) {
                        String suffix = name.substring(srcPrefix.length());
                        pageRow.setField(dstPrefix + suffix, row.getField(name));
                    } else if (name.equals(arrayPrefix + i)) {
                        // scalar array item with no sub-fields
                        pageRow.setField(arrayPrefix + pageLocalIndex, row.getField(name));
                    }
                }
            }

            out.collect(msg.withPayload(pageRow));
        }
    }

    // ── ResultTypeQueryable ───────────────────────────────────────────────────

    @Override
    public TypeInformation<ProcessedMessage> getProducedType() {
        return ProcessedMessageTypeInfo.INSTANCE;
    }
}

