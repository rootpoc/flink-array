package com.pipeline.common;

import java.io.Serializable;

/**
 * Immutable value object that names the four output fields of a paginated-array message:
 * the data array, the current page index, the total page count, and the per-page item count.
 *
 * <p>Produced by {@link com.pipeline.splitting.schema.SchemaAnalyzer} from a JSON Schema
 * and consumed by {@link com.pipeline.splitting.page.PageBuilder} to build output pages.
 */
public final class PaginationSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String arrayFieldName;
    private final String indexFieldName;
    private final String totalFieldName;
    private final String countFieldName;

    public PaginationSchema(
            String arrayFieldName,
            String indexFieldName,
            String totalFieldName,
            String countFieldName) {
        this.arrayFieldName = arrayFieldName;
        this.indexFieldName = indexFieldName;
        this.totalFieldName = totalFieldName;
        this.countFieldName = countFieldName;
    }

    /** Name of the required property whose JSON type is {@code "array"} (e.g. {@code "persons"}). */
    public String getArrayFieldName() { return arrayFieldName; }

    /** Name of the 0-based current-page index field (e.g. {@code "index"}). */
    public String getIndexFieldName() { return indexFieldName; }

    /** Name of the total-pages field (e.g. {@code "total"}). */
    public String getTotalFieldName() { return totalFieldName; }

    /** Name of the per-page item-count field (e.g. {@code "count"}). */
    public String getCountFieldName() { return countFieldName; }

    @Override
    public String toString() {
        return "PaginationSchema{"
                + "array='" + arrayFieldName + '\''
                + ", index='" + indexFieldName + '\''
                + ", total='" + totalFieldName + '\''
                + ", count='" + countFieldName + '\''
                + '}';
    }
}
