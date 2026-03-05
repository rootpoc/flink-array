package com.pipeline.splitting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.PaginationSchema;
import com.pipeline.splitting.page.PageBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PageBuilder}.
 *
 * <p>Pure Jackson — no Flink dependency required.
 */
class PageBuilderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final PaginationSchema SCHEMA =
            new PaginationSchema("persons", "index", "total", "count");

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void build_setsAllFourFields() {
        ArrayNode items = MAPPER.createArrayNode();
        items.addObject().put("name", "Alice");
        items.addObject().put("name", "Bob");

        ObjectNode page = PageBuilder.build(SCHEMA, MAPPER, items, 2, 5);

        assertEquals(2, page.get("index").asInt(),    "index field");
        assertEquals(5, page.get("total").asInt(),    "total field");
        assertEquals(2, page.get("count").asInt(),    "count field");
        assertTrue(page.has("persons"),               "persons array must be present");
    }

    @Test
    void build_preservesArrayContent() {
        ArrayNode items = MAPPER.createArrayNode();
        items.addObject().put("id", 10);
        items.addObject().put("id", 20);
        items.addObject().put("id", 30);

        ObjectNode page = PageBuilder.build(SCHEMA, MAPPER, items, 0, 1);

        ArrayNode personsOut = (ArrayNode) page.get("persons");
        assertNotNull(personsOut, "persons must not be null");
        assertEquals(3, personsOut.size(), "all three items must be present");
        assertEquals(10, personsOut.get(0).get("id").asInt());
        assertEquals(20, personsOut.get(1).get("id").asInt());
        assertEquals(30, personsOut.get(2).get("id").asInt());
    }

    @Test
    void build_countReflectsActualItemsSize() {
        ArrayNode items = MAPPER.createArrayNode();
        // last page: only 1 item even though pageSize might be larger
        items.addObject().put("name", "Last");

        ObjectNode page = PageBuilder.build(SCHEMA, MAPPER, items, 4, 5);

        assertEquals(1, page.get("count").asInt(), "count must match actual slice size");
    }

    @Test
    void build_emptySliceProducesZeroCount() {
        ArrayNode items = MAPPER.createArrayNode();

        ObjectNode page = PageBuilder.build(SCHEMA, MAPPER, items, 0, 0);

        assertEquals(0, page.get("count").asInt(), "empty slice → count 0");
        assertEquals(0, ((ArrayNode) page.get("persons")).size());
    }
}
