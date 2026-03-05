package com.pipeline.splitting.page;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.PaginationSchema;

/**
 * Pure-static utility that constructs a single output page as a Jackson {@link ObjectNode}.
 *
 * <p>The four fields written into the page are determined by the supplied {@link PaginationSchema}
 * so that any conforming schema can be used without code changes.
 */
public final class PageBuilder {

    private PageBuilder() {}

    /**
     * Builds one output page.
     *
     * @param schema     field-name mapping derived from the output JSON Schema
     * @param mapper     {@link ObjectMapper} used to create nodes
     * @param items      the slice of items to include in this page
     * @param pageIndex  0-based index of this page
     * @param totalPages total number of pages in the split
     * @return an {@link ObjectNode} ready for serialisation
     */
    public static ObjectNode build(
            PaginationSchema schema,
            ObjectMapper     mapper,
            ArrayNode        items,
            int              pageIndex,
            int              totalPages) {
        ObjectNode page = mapper.createObjectNode();
        page.put(schema.getIndexFieldName(), pageIndex);
        page.put(schema.getTotalFieldName(), totalPages);
        page.put(schema.getCountFieldName(), items.size());
        page.set(schema.getArrayFieldName(), items);
        return page;
    }
}
