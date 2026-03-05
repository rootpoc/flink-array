package com.pipeline.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.ProcessedMessage;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ReconstructSerializer} using Netflix-schema-shaped {@link Row} data —
 * the exact output produced by {@link com.pipeline.ValidateSplitFlattenFunction}.
 *
 * <p>Each test builds a named {@link Row} that mirrors a flattened Netflix page
 * (pagination metadata + dot-notation item fields) and verifies that the serializer
 * reconstructs the correct hierarchical JSON structure.
 */
class ReconstructSerializerNetflixTest {

    private ReconstructSerializer serializer;
    private ObjectMapper          mapper;

    /** Pagination schema field names used across all tests. */
    private static final String F_INDEX  = "index";
    private static final String F_TOTAL  = "total";
    private static final String F_COUNT  = "count";
    private static final String F_ARRAY  = "search_engines";

    @BeforeEach
    void setUp() throws Exception {
        serializer = new ReconstructSerializer("output-topic");
        serializer.open(null, null);
        mapper = new ObjectMapper();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JsonNode reconstruct(Row row) throws Exception {
        ProducerRecord<byte[], byte[]> record = serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);
        assertNotNull(record);
        return mapper.readTree(record.value());
    }

    /** Build a single-item Netflix page Row with full imdb and list fields. */
    private static Row singleItemPageRow() {
        Row row = Row.withNames();
        row.setField(F_INDEX, 0);
        row.setField(F_TOTAL, 1);
        row.setField(F_COUNT, 1);
        row.setField("search_engines.0.category",          "Movies");
        row.setField("search_engines.0.name",              "IMDB Search");
        row.setField("search_engines.0.pinned",            true);
        row.setField("search_engines.0.last_used",         100);
        row.setField("search_engines.0.list.0",            "tt1234");
        row.setField("search_engines.0.list.1",            "tt5678");
        row.setField("search_engines.0.imdb.id",           "tt1234");
        row.setField("search_engines.0.imdb.title",        "Some Film");
        row.setField("search_engines.0.imdb.year",         "2023");
        row.setField("search_engines.0.imdb.rated",        "PG-13");
        row.setField("search_engines.0.imdb.genre",        "Action");
        row.setField("search_engines.0.imdb.director",     "Jane Doe");
        row.setField("search_engines.0.imdb.actors",       "John Actor");
        row.setField("search_engines.0.imdb.imdb_rating",  7.5);
        return row;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Pagination metadata
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void netflixPage_paginationFieldsAtRootLevel() throws Exception {
        Row row = Row.withNames();
        row.setField(F_INDEX, 2);
        row.setField(F_TOTAL, 5);
        row.setField(F_COUNT, 3);

        JsonNode root = reconstruct(row);

        assertEquals(2, root.get(F_INDEX).intValue(), "index must be at root");
        assertEquals(5, root.get(F_TOTAL).intValue(), "total must be at root");
        assertEquals(3, root.get(F_COUNT).intValue(), "count must be at root");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Array reconstruction
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void netflixPage_searchEnginesIsArray() throws Exception {
        JsonNode root = reconstruct(singleItemPageRow());
        assertTrue(root.path(F_ARRAY).isArray(), "search_engines must be a JSON array");
    }

    @Test
    void netflixPage_itemHasAllExpectedFields() throws Exception {
        JsonNode item = reconstruct(singleItemPageRow()).path(F_ARRAY).get(0);

        assertEquals("Movies",      item.path("category").asText());
        assertEquals("IMDB Search", item.path("name").asText());
        assertEquals(100,           item.path("last_used").intValue());
        assertTrue(item.path("pinned").isBoolean(), "pinned must be a boolean node");
    }

    @Test
    void netflixPage_listArrayReconstructed() throws Exception {
        JsonNode list = reconstruct(singleItemPageRow())
                .path(F_ARRAY).get(0).path("list");

        assertTrue(list.isArray(), "list must be a JSON array");
        assertEquals(2,        list.size(),         "list must have 2 elements");
        assertEquals("tt1234", list.get(0).asText(), "list[0]");
        assertEquals("tt5678", list.get(1).asText(), "list[1]");
    }

    @Test
    void netflixPage_imdbObjectReconstructed() throws Exception {
        JsonNode imdb = reconstruct(singleItemPageRow())
                .path(F_ARRAY).get(0).path("imdb");

        assertTrue(imdb.isObject(), "imdb must be a JSON object");
        assertEquals("tt1234",    imdb.path("id").asText());
        assertEquals("Some Film", imdb.path("title").asText());
        assertEquals("Jane Doe",  imdb.path("director").asText());
        assertEquals("John Actor",imdb.path("actors").asText());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Type fidelity
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void netflixPage_imdbRatingIsNumberType() throws Exception {
        JsonNode imdb = reconstruct(singleItemPageRow())
                .path(F_ARRAY).get(0).path("imdb");

        assertTrue(imdb.path("imdb_rating").isNumber(), "imdb_rating must be a number node");
        assertEquals(7.5, imdb.path("imdb_rating").doubleValue(), 0.001);
    }

    @Test
    void netflixPage_pinnedIsBooleanType() throws Exception {
        JsonNode item = reconstruct(singleItemPageRow()).path(F_ARRAY).get(0);
        assertTrue(item.path("pinned").isBoolean(), "pinned must be a boolean node");
        assertTrue(item.path("pinned").booleanValue());
    }

    @Test
    void netflixPage_lastUsedIsIntegerType() throws Exception {
        JsonNode item = reconstruct(singleItemPageRow()).path(F_ARRAY).get(0);
        assertTrue(item.path("last_used").isInt(), "last_used must be an integer node");
        assertEquals(100, item.path("last_used").intValue());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Multiple items in one page
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void multipleItems_allReconstructedInArray() throws Exception {
        Row row = Row.withNames();
        row.setField(F_INDEX, 0);
        row.setField(F_TOTAL, 1);
        row.setField(F_COUNT, 2);
        row.setField("search_engines.0.category", "Movies");
        row.setField("search_engines.0.name",     "Engine A");
        row.setField("search_engines.0.pinned",   true);
        row.setField("search_engines.0.last_used", 0);
        row.setField("search_engines.1.category", "Series");
        row.setField("search_engines.1.name",     "Engine B");
        row.setField("search_engines.1.pinned",   false);
        row.setField("search_engines.1.last_used", 1);

        JsonNode arr = reconstruct(row).path(F_ARRAY);

        assertTrue(arr.isArray());
        assertEquals(2,          arr.size(),                    "must have 2 items");
        assertEquals("Engine A", arr.get(0).path("name").asText(), "item 0 name");
        assertEquals("Engine B", arr.get(1).path("name").asText(), "item 1 name");
        assertTrue(arr.get(0).path("pinned").booleanValue(),   "item 0 pinned");
        assertFalse(arr.get(1).path("pinned").booleanValue(),  "item 1 pinned");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Post-processing — uppercased director
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void directorUppercased_appearsUppercasedInOutputJson() throws Exception {
        Row row = singleItemPageRow();
        // Simulate what UpperCaseMapFunction does
        row.setField("search_engines.0.imdb.director", "JANE DOE");

        JsonNode director = reconstruct(row)
                .path(F_ARRAY).get(0).path("imdb").path("director");

        assertEquals("JANE DOE", director.asText(), "uppercased director must appear in JSON");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // With array — structural verification
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void withArray_indexedPaths_producesJsonArray() throws Exception {
        // Any row with numeric path segments must produce a JSON array node
        Row row = Row.withNames();
        row.setField("items.0.name", "first");
        row.setField("items.1.name", "second");

        JsonNode arr = reconstruct(row).path("items");
        assertTrue(arr.isArray(), "numeric path segments must produce a JSON array");
        assertEquals(2, arr.size());
    }

    @Test
    void withArray_nestedSubArray_reconstructedInsideObject() throws Exception {
        // list.0, list.1 inside a search_engine item must become a sub-array
        Row row = Row.withNames();
        row.setField("search_engines.0.name",   "Engine");
        row.setField("search_engines.0.list.0", "url0");
        row.setField("search_engines.0.list.1", "url1");
        row.setField("search_engines.0.list.2", "url2");

        JsonNode list = reconstruct(row).path(F_ARRAY).get(0).path("list");
        assertTrue(list.isArray(), "list must be a JSON array");
        assertEquals(3, list.size());
        assertEquals("url2", list.get(2).asText());
    }

    @Test
    void withArray_singleElementArray_hasExactlyOneElement() throws Exception {
        Row row = Row.withNames();
        row.setField("results.0.value", "only");

        JsonNode arr = reconstruct(row).path("results");
        assertTrue(arr.isArray());
        assertEquals(1, arr.size(), "single indexed element must yield a one-element array");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Without array — flat / object-only rows
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void withoutArray_flatRow_noArraysInOutput() throws Exception {
        // Row with only non-numeric path segments → no JSON arrays anywhere
        Row row = Row.withNames();
        row.setField("application.name",    "Netflix");
        row.setField("application.app_id",  "id-001");
        row.setField("application.version", "v1");
        row.setField("index", 0);
        row.setField("total", 1);
        row.setField("count", 0);

        JsonNode root = reconstruct(row);

        assertTrue(root.path("application").isObject(), "application must be an object");
        assertEquals("Netflix", root.at("/application/name").asText());
        assertEquals("id-001",  root.at("/application/app_id").asText());
        assertFalse(root.path("application").isArray(), "no numeric segments → no array");
    }

    @Test
    void withoutArray_emptyRow_producesEmptyJsonObject() throws Exception {
        Row row = Row.withNames();
        JsonNode root = reconstruct(row);
        assertTrue(root.isObject(), "empty row must produce an object");
        assertEquals(0, root.size(), "empty row must produce an empty object");
    }

    @Test
    void withoutArray_deeplyNestedObjectOnly_noArrays() throws Exception {
        // Pure object nesting without any numeric segments
        Row row = Row.withNames();
        row.setField("a.b.c.d.e", "deep");

        JsonNode root = reconstruct(row);
        assertEquals("deep", root.at("/a/b/c/d/e").asText());
        // None of the intermediate nodes should be arrays
        assertTrue(root.path("a").isObject());
        assertTrue(root.at("/a/b").isObject());
        assertTrue(root.at("/a/b/c").isObject());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Missing fields in Row — serializer must handle gracefully
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void missingField_rowWithOnlyPaginationFields_searchEnginesAbsent() throws Exception {
        // Row that only has pagination metadata but no array items at all
        Row row = Row.withNames();
        row.setField(F_INDEX, 0);
        row.setField(F_TOTAL, 1);
        row.setField(F_COUNT, 0);

        JsonNode root = reconstruct(row);
        assertEquals(0, root.path(F_INDEX).intValue());
        assertEquals(1, root.path(F_TOTAL).intValue());
        assertEquals(0, root.path(F_COUNT).intValue());
        assertFalse(root.has(F_ARRAY), "search_engines must not appear when no items were set");
    }

    @Test
    void missingField_imdbAbsent_itemStillReconstructed() throws Exception {
        // Row with item fields but no imdb sub-object — item must still be serialised
        Row row = Row.withNames();
        row.setField(F_INDEX, 0);
        row.setField(F_TOTAL, 1);
        row.setField(F_COUNT, 1);
        row.setField("search_engines.0.category", "Movies");
        row.setField("search_engines.0.name",     "Engine");
        row.setField("search_engines.0.pinned",   true);
        row.setField("search_engines.0.last_used", 0);

        JsonNode item = reconstruct(row).path(F_ARRAY).get(0);
        assertEquals("Movies", item.path("category").asText());
        assertFalse(item.has("imdb"), "imdb must not appear when not set in Row");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Extra fields in Row — must be forwarded into output JSON
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void extraFields_inRow_appearsInOutputJson() throws Exception {
        // Fields beyond the schema's required set must be reconstructed as-is
        Row row = singleItemPageRow();
        row.setField("search_engines.0.rank",        1);
        row.setField("search_engines.0.extra_score", 9.9);

        JsonNode item = reconstruct(row).path(F_ARRAY).get(0);
        assertEquals(1,   item.path("rank").intValue(),        "extra int field must appear");
        assertEquals(9.9, item.path("extra_score").doubleValue(), 0.001, "extra double must appear");
    }

    @Test
    void extraFields_topLevelExtra_appearsInOutputJson() throws Exception {
        // Extra top-level fields from the input (e.g. application object) must be serialised
        Row row = Row.withNames();
        row.setField(F_INDEX, 0);
        row.setField(F_TOTAL, 1);
        row.setField(F_COUNT, 1);
        row.setField("search_engines.0.name", "E");
        row.setField("application.name",      "Netflix");
        row.setField("application.version",   "v1");

        JsonNode root = reconstruct(row);
        assertTrue(root.path("application").isObject(), "application must be serialised");
        assertEquals("Netflix", root.at("/application/name").asText());
    }

    @Test
    void withoutArray_itemsWithPrimitivesOnly_noSubArrays() throws Exception {
        // Netflix page where items have only scalar fields (no list sub-array, no imdb object)
        Row row = Row.withNames();
        row.setField(F_INDEX, 0);
        row.setField(F_TOTAL, 1);
        row.setField(F_COUNT, 2);
        row.setField("search_engines.0.category", "Movies");
        row.setField("search_engines.0.name",     "Engine A");
        row.setField("search_engines.0.pinned",   true);
        row.setField("search_engines.0.last_used", 10);
        row.setField("search_engines.1.category", "Series");
        row.setField("search_engines.1.name",     "Engine B");
        row.setField("search_engines.1.pinned",   false);
        row.setField("search_engines.1.last_used", 20);

        JsonNode root = reconstruct(row);
        JsonNode item0 = root.path(F_ARRAY).get(0);
        JsonNode item1 = root.path(F_ARRAY).get(1);

        // Items must be objects with scalar fields — no nested arrays
        assertTrue(item0.isObject());
        assertFalse(item0.has("list"), "no list sub-array expected");
        assertFalse(item0.has("imdb"), "no imdb sub-object expected");
        assertEquals("Engine A", item0.path("name").asText());
        assertEquals("Engine B", item1.path("name").asText());
    }
}
