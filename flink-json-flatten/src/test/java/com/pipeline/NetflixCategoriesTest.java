package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.deserialization.FlatteningDeserializer;
import com.pipeline.serialization.ReconstructSerializer;
import com.pipeline.common.typeinfo.FlatRowSerializer;
import com.pipeline.splitting.ArraySplitterFunction;
import com.pipeline.common.PaginationSchema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests using real Netflix categories data.
 *
 * <h2>Scenario A — no splitting: input schema == output schema</h2>
 * The Netflix JSON is passed directly through {@link FlatteningDeserializer} without any
 * array splitting.  The whole document is flattened into one {@link Row} and reconstructed
 * back to JSON, which must be structurally equal to the original input.
 * "Input schema equals output schema" because the structure is preserved end-to-end.
 *
 * <h2>Scenario B — split on {@code search_engines}: one input → multiple output messages</h2>
 * {@link ArraySplitterFunction} pages the 5-element {@code search_engines} array with
 * {@code pageSize=2}, producing three paginated JSON messages (counts: 2, 2, 1).
 * Each page is then flattened independently, demonstrating that the pipeline produces
 * multiple downstream messages from a single incoming message.
 */
class NetflixCategoriesTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Test fixture ─────────────────────────────────────────────────────────

    static final String NETFLIX_JSON = "{"
            + "\"application\":{\"name\":\"Netflix Categories\",\"app_id\":\"netflix-categories@deekshith.in\",\"version\":\"v0.1\"},"
            + "\"search_engines\":["
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action & Adventure\","
            + "\"list\":[\"http://www.netflix.com/browse/genre/1365\",\"https://www.netflix.com/title/genre/1365\"],"
            + "\"pinned\":true,\"last_used\":0,\"imdb\":{\"id\":\"tt8936646\",\"title\":\"Extraction\",\"year\":\"2020\","
            + "\"rated\":\"R\",\"genre\":\"Action, Thriller\",\"director\":\"Sam Hargrave\","
            + "\"actors\":\"Chris Hemsworth, Rudhraksh Jaiswal\",\"imdb_rating\":6.8}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action Comedies\","
            + "\"list\":[\"http://www.netflix.com/browse/genre/43040\",\"https://www.netflix.com/browse/genre/43040?region=il\"],"
            + "\"pinned\":false,\"last_used\":0,\"imdb\":{\"id\":\"tt4158476\",\"title\":\"Deadpool 2\",\"year\":\"2018\","
            + "\"rated\":\"R\",\"genre\":\"Action, Adventure, Comedy\",\"director\":\"David Leitch\","
            + "\"actors\":\"Ryan Reynolds, Josh Brolin\",\"imdb_rating\":7.6}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action Sci-Fi & Fantasy\","
            + "\"list\":[\"http://www.netflix.com/browse/genre/1568\",\"https://www.netflix.com/genre/1568\"],"
            + "\"pinned\":false,\"last_used\":0,\"imdb\":{\"id\":\"tt1392190\",\"title\":\"Mad Max: Fury Road\",\"year\":\"2015\","
            + "\"rated\":\"R\",\"genre\":\"Action, Adventure, Sci-Fi\",\"director\":\"George Miller\","
            + "\"actors\":\"Charlize Theron, Tom Hardy\",\"imdb_rating\":8.1}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Horror Movies\","
            + "\"list\":[\"http://www.netflix.com/browse/genre/8711\",\"https://www.netflix.com/title/genre/8711\"],"
            + "\"pinned\":false,\"last_used\":0,\"imdb\":{\"id\":\"tt0365748\",\"title\":\"The Descent\",\"year\":\"2005\","
            + "\"rated\":\"R\",\"genre\":\"Adventure, Horror\",\"director\":\"Neil Marshall\","
            + "\"actors\":\"Shauna Macdonald, Natalie Mendoza\",\"imdb_rating\":7.2}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Anime Series\","
            + "\"list\":[\"http://www.netflix.com/browse/genre/7424\",\"https://www.netflix.com/browse/genre/7424\"],"
            + "\"pinned\":true,\"last_used\":12,\"imdb\":{\"id\":\"tt14947990\",\"title\":\"Cyberpunk: Edgerunners\",\"year\":\"2022\","
            + "\"rated\":\"TV-MA\",\"genre\":\"Animation, Action, Sci-Fi\",\"director\":\"Ibon Cormenzana\","
            + "\"actors\":\"Zach Aguilar, Emi Lo\",\"imdb_rating\":8.3}}]}";

    static final byte[] NETFLIX_BYTES = NETFLIX_JSON.strip().getBytes(StandardCharsets.UTF_8);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<Row> flattenBytes(byte[] input) throws Exception {
        var deserializer = new FlatteningDeserializer(
                new com.pipeline.config.PipelineConfig.Builder().build());
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(deserializer));
        harness.setup(FlatRowSerializer.INSTANCE);
        harness.open();
        harness.processElement(input, System.currentTimeMillis());
        List<Row> rows = harness.extractOutputValues();
        harness.close();
        return rows;
    }

    private static List<byte[]> splitBytes(byte[] input, String arrayField,
                                           PaginationSchema schema, int pageSize)
            throws Exception {
        var splitter = new ArraySplitterFunction(arrayField, pageSize, schema);
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(splitter));
        harness.setup(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
                .createSerializer(new ExecutionConfig()));
        harness.open();
        harness.processElement(input, System.currentTimeMillis());
        List<byte[]> pages = harness.extractOutputValues();

        // Verify no DLQ records (sanity check)
        Queue<?> rawDlq = harness.getSideOutput(ArraySplitterFunction.DLQ_TAG);
        int dlqCount = rawDlq == null ? 0 : rawDlq.size();
        assertEquals(0, dlqCount, "Splitter must not produce DLQ records for valid input");

        harness.close();
        return pages;
    }

    private static Set<String> fieldNames(Row row) {
        Set<String> names = row.getFieldNames(false);
        return names != null ? names : Set.of();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Scenario A — input schema == output schema (no splitting)
    //
    // The Netflix JSON is flattened directly into a single Row.
    // The schema is "the same" end-to-end: no structural transformation is
    // applied, and round-tripping through ReconstructSerializer reproduces
    // the original JSON unchanged.
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void scenarioA_noSplitting_singleInputProducesSingleFlatRow() throws Exception {
        List<Row> rows = flattenBytes(NETFLIX_BYTES);

        assertEquals(1, rows.size(), "one Row expected for the whole document");
        Row row = rows.get(0);

        // Application fields
        assertEquals("Netflix Categories",              row.getField("application.name"));
        assertEquals("netflix-categories@deekshith.in", row.getField("application.app_id"));
        assertEquals("v0.1",                            row.getField("application.version"));

        // First search engine (index 0)
        assertEquals("NetflixCategories",  row.getField("search_engines.0.category"));
        assertEquals("Action & Adventure", row.getField("search_engines.0.name"));
        assertEquals(true,                 row.getField("search_engines.0.pinned"));
        assertEquals(0L,                   row.getField("search_engines.0.last_used"));
        assertEquals("http://www.netflix.com/browse/genre/1365",
                row.getField("search_engines.0.list.0"));

        // Last search engine (index 4)
        assertEquals("Anime Series",        row.getField("search_engines.4.name"));
        assertEquals(true,                  row.getField("search_engines.4.pinned"));
        assertEquals(12L,                   row.getField("search_engines.4.last_used"));
        assertEquals("Cyberpunk: Edgerunners", row.getField("search_engines.4.imdb.title"));
        assertEquals(8.3d,                  row.getField("search_engines.4.imdb.imdb_rating"));

        // IMDB fields for engine 2 (Mad Max)
        assertEquals("Mad Max: Fury Road",  row.getField("search_engines.2.imdb.title"));
        assertEquals("8.1",                 String.valueOf(row.getField("search_engines.2.imdb.imdb_rating")));
    }

    @Test
    void scenarioA_roundTrip_reconstructedJsonEqualsOriginalInput() throws Exception {
        List<Row> rows = flattenBytes(NETFLIX_BYTES);
        Row row = rows.get(0);

        ReconstructSerializer reconstructor = new ReconstructSerializer("test-topic");
        reconstructor.open(null, null);
        ProducerRecord<byte[], byte[]> record = reconstructor.serialize(ProcessedMessage.ofPayload(row), null, null);
        assertNotNull(record);

        JsonNode expected = MAPPER.readTree(NETFLIX_JSON.strip());
        JsonNode actual   = MAPPER.readTree(record.value());

        assertEquals(expected, actual,
                "Reconstructed JSON must be structurally equal to the original Netflix input.\n"
                + "actual:\n" + actual.toPrettyString());
    }

    @Test
    void scenarioA_flatRowFieldCount_matchesExpectedTotalLeaves() throws Exception {
        // 3 application fields
        // 5 search engines × (4 scalar fields + 2 list entries + 8 imdb fields) = 5 × 14 = 70
        // Total: 73 leaf fields
        List<Row> rows = flattenBytes(NETFLIX_BYTES);
        int fieldCount = fieldNames(rows.get(0)).size();

        System.out.println("\n=== Netflix flat row (" + fieldCount + " fields) ===");
        fieldNames(rows.get(0)).stream().sorted().forEach(name ->
                System.out.printf("  %-55s = %s%n", name,
                        rows.get(0).getField(name)));
        System.out.println("=================================================\n");

        assertEquals(73, fieldCount,
                "Expected 3 app fields + 5 × 14 search-engine leaf fields = 73 total");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Scenario B — split on search_engines: one input → multiple output messages
    //
    // The paginated output schema uses field names that differ from the input,
    // proving the schema-agnostic nature of ArraySplitterFunction.
    // 5 search engines, pageSize=2 → ceil(5/2) = 3 pages with counts [2, 2, 1].
    // ══════════════════════════════════════════════════════════════════════════

    /** Paginated schema for the Netflix splitter — field names are deliberately distinct. */
    private static final PaginationSchema NETFLIX_PAGINATED_SCHEMA =
            new PaginationSchema("search_engines", "page_index", "total_pages", "page_count");

    @Test
    void scenarioB_splitProducesThreePagesFromFiveEngines() throws Exception {
        List<byte[]> pages = splitBytes(
                NETFLIX_BYTES, "search_engines", NETFLIX_PAGINATED_SCHEMA, 2);

        assertEquals(3, pages.size(), "ceil(5/2) = 3 pages expected");
    }

    @Test
    void scenarioB_paginationMetadataIsCorrectOnAllPages() throws Exception {
        List<byte[]> pages = splitBytes(
                NETFLIX_BYTES, "search_engines", NETFLIX_PAGINATED_SCHEMA, 2);

        int[] expectedCounts = {2, 2, 1};
        for (int i = 0; i < pages.size(); i++) {
            JsonNode page = MAPPER.readTree(pages.get(i));
            assertEquals(i,                  page.get("page_index").asInt(),  "page_index");
            assertEquals(3,                  page.get("total_pages").asInt(), "total_pages");
            assertEquals(expectedCounts[i],  page.get("page_count").asInt(),  "page_count");
            assertTrue(page.get("search_engines").isArray(),                  "search_engines must be array");
            assertEquals(expectedCounts[i],  page.get("search_engines").size(),"array size");
        }
    }

    @Test
    void scenarioB_firstPageContainsCorrectSearchEngines() throws Exception {
        List<byte[]> pages = splitBytes(
                NETFLIX_BYTES, "search_engines", NETFLIX_PAGINATED_SCHEMA, 2);

        JsonNode page0 = MAPPER.readTree(pages.get(0));
        JsonNode engines = page0.get("search_engines");

        assertEquals("Action & Adventure",  engines.get(0).get("name").asText());
        assertEquals("Action Comedies",     engines.get(1).get("name").asText());
    }

    @Test
    void scenarioB_lastPageHasOneEngineWithCorrectData() throws Exception {
        List<byte[]> pages = splitBytes(
                NETFLIX_BYTES, "search_engines", NETFLIX_PAGINATED_SCHEMA, 2);

        JsonNode lastPage    = MAPPER.readTree(pages.get(2));
        JsonNode lastEngines = lastPage.get("search_engines");

        assertEquals(1, lastEngines.size());
        assertEquals("Anime Series", lastEngines.get(0).get("name").asText());
        assertEquals(12,             lastEngines.get(0).get("last_used").asInt());
        assertEquals(8.3d,           lastEngines.get(0).get("imdb").get("imdb_rating").asDouble(),
                1e-9);
    }

    @Test
    void scenarioB_eachPageCanBeFlattenedIndependently() throws Exception {
        List<byte[]> pages = splitBytes(
                NETFLIX_BYTES, "search_engines", NETFLIX_PAGINATED_SCHEMA, 2);

        // Each page produces exactly one Row — no DLQ
        for (int i = 0; i < pages.size(); i++) {
            List<Row> rows = flattenBytes(pages.get(i));
            assertEquals(1, rows.size(),
                    "page " + i + " must flatten into exactly one Row");
            Row row = rows.get(0);
            // Pagination metadata is present as scalar fields.
            // FlatRowSerializer promotes all integral values to Long on round-trip.
            assertEquals((long) i, row.getField("page_index"),
                    "page_index field in flattened row for page " + i);
            assertEquals(3L, row.getField("total_pages"),
                    "total_pages field must be 3 in every row");
        }
    }

    @Test
    void scenarioB_allFiveEnginesReconstructedAcrossPages() throws Exception {
        List<byte[]> pages = splitBytes(
                NETFLIX_BYTES, "search_engines", NETFLIX_PAGINATED_SCHEMA, 2);

        // Collect the name of every search engine across all pages
        List<String> names = new ArrayList<>();
        for (byte[] pageBytes : pages) {
            JsonNode page = MAPPER.readTree(pageBytes);
            page.get("search_engines").forEach(e -> names.add(e.get("name").asText()));
        }

        assertEquals(5, names.size(), "all five search engines must appear across pages");
        assertEquals("Action & Adventure",    names.get(0));
        assertEquals("Action Comedies",       names.get(1));
        assertEquals("Action Sci-Fi & Fantasy", names.get(2));
        assertEquals("Horror Movies",         names.get(3));
        assertEquals("Anime Series",          names.get(4));
    }
}
