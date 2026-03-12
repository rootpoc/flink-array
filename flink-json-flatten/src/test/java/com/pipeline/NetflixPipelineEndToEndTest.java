package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.processing.UpperCaseMapFunction;
import com.pipeline.serialization.ReconstructSerializer;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.common.PaginationSchema;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for the Netflix pipeline:
 * <pre>
 *   byte[] (raw JSON)
 *     → ValidateSplitFlattenFunction  (validate + split + flatten → Row per page)
 *     → UpperCaseMapFunction          (uppercase director field)
 *     → ReconstructSerializer         (Row → hierarchical JSON bytes)
 * </pre>
 *
 * <p>Each test drives real JSON through all three stages and asserts on the
 * final reconstructed JSON, verifying correctness of the full pipeline.
 */
class NetflixPipelineEndToEndTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Schemas used throughout — matching input-netflix-categories structure. */
    private static final InputSchemaInfo INPUT_SCHEMA = new InputSchemaInfo(
            "search_engines",
            List.of("application", "search_engines"),
            List.of("category", "name", "list", "pinned", "last_used"));

    private static final PaginationSchema OUTPUT_SCHEMA =
            new PaginationSchema("search_engines", "index", "total", "count");

    /** Pattern that uppercases every director across all search_engines array elements. */
    private static final List<String> DIRECTOR_PATTERN =
            List.of("search_engines.*.imdb.director");

    private ReconstructSerializer serializer;

    @BeforeEach
    void setUp() throws Exception {
        serializer = new ReconstructSerializer("output-topic");
        serializer.open(null, null);
    }

    // ── Fixtures ──────────────────────────────────────────────────────────────

    private static final String ONE_ENGINE_JSON = "{"
            + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"netflix-001\",\"version\":\"v1\"},"
            + "\"search_engines\":[{"
            + "\"category\":\"Movies\",\"name\":\"IMDB\","
            + "\"list\":[\"url0\",\"url1\"],\"pinned\":true,\"last_used\":42,"
            + "\"imdb\":{\"id\":\"tt1\",\"title\":\"Some Film\",\"year\":\"2023\","
            + "\"rated\":\"PG\",\"genre\":\"Action\",\"director\":\"Jane Doe\","
            + "\"actors\":\"Actor A\",\"imdb_rating\":7.5}}]}";

    private static final String THREE_ENGINE_JSON = "{"
            + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
            + "\"search_engines\":["
            + "{\"category\":\"C\",\"name\":\"Alpha\",\"list\":[\"u0\"],\"pinned\":true,\"last_used\":0,"
            + "\"imdb\":{\"director\":\"Dir A\",\"title\":\"Film A\",\"id\":\"tt1\",\"year\":\"2020\","
            + "\"rated\":\"PG\",\"genre\":\"G\",\"actors\":\"A\",\"imdb_rating\":7.0}},"
            + "{\"category\":\"C\",\"name\":\"Beta\",\"list\":[\"u1\"],\"pinned\":false,\"last_used\":1,"
            + "\"imdb\":{\"director\":\"Dir B\",\"title\":\"Film B\",\"id\":\"tt2\",\"year\":\"2021\","
            + "\"rated\":\"PG\",\"genre\":\"G\",\"actors\":\"B\",\"imdb_rating\":8.0}},"
            + "{\"category\":\"C\",\"name\":\"Gamma\",\"list\":[\"u2\"],\"pinned\":true,\"last_used\":2,"
            + "\"imdb\":{\"director\":\"Dir C\",\"title\":\"Film C\",\"id\":\"tt3\",\"year\":\"2022\","
            + "\"rated\":\"PG\",\"genre\":\"G\",\"actors\":\"C\",\"imdb_rating\":9.0}}]}";

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Runs the full pipeline for a single input message and returns the final
     * reconstructed JSON for each output page.
     */
    private List<JsonNode> runPipeline(String inputJson,
                                       int pageSize,
                                       List<String> uppercasePatterns) throws Exception {
        byte[] bytes = inputJson.strip().getBytes(StandardCharsets.UTF_8);

        // Stage 1 — validate + split + flatten
        ValidateSplitFlattenFunction stage1 = new ValidateSplitFlattenFunction(
                INPUT_SCHEMA, OUTPUT_SCHEMA, pageSize, NullHandling.INCLUDE);
        var harness1 = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(stage1));
        harness1.setup(ProcessedMessageSerializer.INSTANCE);
        harness1.open();
        harness1.processElement(ProcessedMessage.ofValue(bytes), System.currentTimeMillis());
        List<ProcessedMessage> msgs1 = harness1.extractOutputValues();
        harness1.close();

        // Stage 2 — uppercase map
        PipelineConfig config = new PipelineConfig.Builder()
                .uppercaseFieldKeys(uppercasePatterns)
                .build();
        UpperCaseMapFunction stage2 = new UpperCaseMapFunction(config);
        var harness2 = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(stage2));
        harness2.setup(ProcessedMessageSerializer.INSTANCE);
        harness2.open();
        for (ProcessedMessage msg : msgs1) {
            harness2.processElement(msg, System.currentTimeMillis());
        }
        List<ProcessedMessage> msgs2 = harness2.extractOutputValues();
        harness2.close();

        // Stage 3 — reconstruct JSON
        List<JsonNode> results = new ArrayList<>();
        for (ProcessedMessage msg : msgs2) {
            ProducerRecord<byte[], byte[]> record = serializer.serialize(msg, null, 0L);
            assertNotNull(record);
            results.add(MAPPER.readTree(record.value()));
        }
        return results;
    }

    /** Collect DLQ records from stage 1. */
    private List<DlqRecord> runAndGetDlq(String inputJson) throws Exception {
        byte[] bytes = inputJson.strip().getBytes(StandardCharsets.UTF_8);
        ValidateSplitFlattenFunction stage1 = new ValidateSplitFlattenFunction(
                INPUT_SCHEMA, OUTPUT_SCHEMA, 10, NullHandling.INCLUDE);
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(stage1));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(bytes), System.currentTimeMillis());
        Queue<?> rawDlq = harness.getSideOutput(ValidateSplitFlattenFunction.DLQ_TAG);
        List<DlqRecord> dlq = new ArrayList<>();
        if (rawDlq != null) {
            rawDlq.forEach(sr -> dlq.add(
                    (DlqRecord) ((StreamRecord<?>) sr).getValue()));
        }
        harness.close();
        return dlq;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Structure
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void singleItem_outputJsonHasCorrectStructure() throws Exception {
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, List.of());

        assertEquals(1, pages.size(), "one item → one page");
        JsonNode root = pages.get(0);

        assertTrue(root.has("index"),          "root must have 'index'");
        assertTrue(root.has("total"),          "root must have 'total'");
        assertTrue(root.has("count"),          "root must have 'count'");
        assertTrue(root.path("search_engines").isArray(), "search_engines must be array");
    }

    @Test
    void multipleItems_splitIntoPages_paginationCorrect() throws Exception {
        // pageSize=2, 3 items → 2 pages
        List<JsonNode> pages = runPipeline(THREE_ENGINE_JSON, 2, List.of());

        assertEquals(2, pages.size(), "ceil(3/2) = 2 pages");

        assertEquals(0, pages.get(0).path("index").intValue(), "first page index");
        assertEquals(2, pages.get(0).path("total").intValue(), "total pages");
        assertEquals(2, pages.get(0).path("count").intValue(), "first page count");

        assertEquals(1, pages.get(1).path("index").intValue(), "second page index");
        assertEquals(1, pages.get(1).path("count").intValue(), "second page count");
    }

    @Test
    void paginationFields_presentInEveryOutputPage() throws Exception {
        List<JsonNode> pages = runPipeline(THREE_ENGINE_JSON, 2, List.of());
        for (JsonNode page : pages) {
            assertTrue(page.has("index"), "each page must have 'index'");
            assertTrue(page.has("total"), "each page must have 'total'");
            assertTrue(page.has("count"), "each page must have 'count'");
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // UpperCaseMapFunction integration
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void directorUppercased_inFinalOutputJson() throws Exception {
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, DIRECTOR_PATTERN);
        JsonNode director = pages.get(0)
                .path("search_engines").get(0).path("imdb").path("director");
        assertEquals("JANE DOE", director.asText(), "director must be uppercased");
    }

    @Test
    void nonDirectorFields_unchangedInFinalOutputJson() throws Exception {
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, DIRECTOR_PATTERN);
        JsonNode item = pages.get(0).path("search_engines").get(0);

        assertEquals("Movies",    item.path("category").asText(),       "category unchanged");
        assertEquals("IMDB",      item.path("name").asText(),           "name unchanged");
        assertEquals("Some Film", item.path("imdb").path("title").asText(), "title unchanged");
        assertEquals("Action",    item.path("imdb").path("genre").asText(), "genre unchanged");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Nested structure preservation
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void listArray_preservedThroughFullPipeline() throws Exception {
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, List.of());
        JsonNode list = pages.get(0).path("search_engines").get(0).path("list");

        assertTrue(list.isArray(), "list must be a JSON array");
        assertEquals(2,      list.size(),           "list must have 2 elements");
        assertEquals("url0", list.get(0).asText(), "list[0]");
        assertEquals("url1", list.get(1).asText(), "list[1]");
    }

    @Test
    void imdbObject_preservedThroughFullPipeline() throws Exception {
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, List.of());
        JsonNode imdb = pages.get(0).path("search_engines").get(0).path("imdb");

        assertTrue(imdb.isObject(), "imdb must be a JSON object");
        assertEquals("tt1",  imdb.path("id").asText());
        assertEquals("2023", imdb.path("year").asText());
        assertEquals(7.5,    imdb.path("imdb_rating").doubleValue(), 0.001);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Page content
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void multiplePages_itemsDistributedCorrectlyAcrossPages() throws Exception {
        // pageSize=2, 3 items → page 0: [Alpha, Beta], page 1: [Gamma]
        List<JsonNode> pages = runPipeline(THREE_ENGINE_JSON, 2, List.of());

        JsonNode page0Engines = pages.get(0).path("search_engines");
        assertEquals(2,       page0Engines.size(),                    "page 0 has 2 items");
        assertEquals("Alpha", page0Engines.get(0).path("name").asText(), "page 0 item 0");
        assertEquals("Beta",  page0Engines.get(1).path("name").asText(), "page 0 item 1");

        JsonNode page1Engines = pages.get(1).path("search_engines");
        assertEquals(1,       page1Engines.size(),                    "page 1 has 1 item");
        assertEquals("Gamma", page1Engines.get(0).path("name").asText(), "page 1 item 0");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Validation failure
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void invalidMessage_missingRequiredField_routedToDlq() throws Exception {
        // Item is missing required field "category" — whole message must be rejected to DLQ
        String invalid = "{"
                + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"name\":\"E\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0}]}";
        List<DlqRecord> dlq = runAndGetDlq(invalid);

        assertFalse(dlq.isEmpty(), "invalid message must go to DLQ");
        assertNotNull(dlq.get(0).getErrorMessage(), "DLQ record must have an error message");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Missing required fields — end-to-end DLQ routing
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void missingRequired_allTopLevelFieldsMissing_routedToDlq() throws Exception {
        List<DlqRecord> dlq = runAndGetDlq("{}");
        assertFalse(dlq.isEmpty(), "empty message must go to DLQ");
        String msg = dlq.get(0).getErrorMessage();
        assertTrue(msg.contains("application")    || msg.contains("search_engines"),
                "error must name a missing field");
    }

    @Test
    void missingRequired_itemMissingRequiredField_noPagesEmitted() throws Exception {
        // Item is missing "category" — whole message must be rejected
        String invalid = "{"
                + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"name\":\"E\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0}]}";
        List<JsonNode> pages = runPipeline(invalid, 10, List.of());
        assertTrue(pages.isEmpty(), "message with invalid item must produce no output pages");
    }

    @Test
    void missingRequired_partialTopLevel_onlyApplicationPresent_routedToDlq() throws Exception {
        String invalid = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"}}";
        List<DlqRecord> dlq = runAndGetDlq(invalid);
        assertFalse(dlq.isEmpty());
        assertTrue(dlq.get(0).getErrorMessage().contains("search_engines"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Additional (extra) fields — must flow through, not cause DLQ
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void extraFields_atTopLevel_messageProceedsNormally() throws Exception {
        // Extra top-level fields must not trigger validation failure
        String json = "{"
                + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"category\":\"C\",\"name\":\"E\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0}],"
                + "\"metadata\":{\"source\":\"kafka\",\"offset\":42},\"pipeline_version\":\"2.0\"}";
        List<JsonNode> pages = runPipeline(json, 10, List.of());
        assertFalse(pages.isEmpty(), "message with extra top-level fields must produce output");
        assertTrue(pages.get(0).has("search_engines"), "normal output must still be present");
    }

    @Test
    void extraFields_insideItems_messageProceedsNormally() throws Exception {
        // Items with extra fields must not trigger validation failure
        String json = "{"
                + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"category\":\"Movies\",\"name\":\"Engine\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,"
                + "\"rank\":5,\"score\":9.5,\"tags\":[\"action\",\"drama\"]}]}";
        List<JsonNode> pages = runPipeline(json, 10, List.of());
        assertFalse(pages.isEmpty(), "message with extra item fields must produce output");
    }

    @Test
    void extraFields_requiredFieldsStillPresentInOutputJson() throws Exception {
        // Required fields must appear correctly in the output even when extras are present
        String json = "{"
                + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"category\":\"Movies\",\"name\":\"IMDB\",\"list\":[\"url0\"],\"pinned\":true,\"last_used\":7,"
                + "\"bonus_field\":\"extra\",\"score\":8}]}";
        List<JsonNode> pages = runPipeline(json, 10, List.of());
        JsonNode item = pages.get(0).path("search_engines").get(0);
        assertEquals("Movies", item.path("category").asText(), "category must be in output");
        assertEquals("IMDB",   item.path("name").asText(),     "name must be in output");
        assertTrue(item.path("pinned").booleanValue(),         "pinned must be in output");
        assertEquals(7,        item.path("last_used").intValue(),"last_used must be in output");
    }

    @Test
    void extraFields_extraAndRequiredMixed_pagingUnaffected() throws Exception {
        // Paging logic must be unaffected by extra fields in message
        String json = "{"
                + "\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"extra_top\":\"ignored\","
                + "\"search_engines\":["
                + "{\"category\":\"C\",\"name\":\"E0\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,\"rank\":1},"
                + "{\"category\":\"C\",\"name\":\"E1\",\"list\":[\"u\"],\"pinned\":false,\"last_used\":1,\"rank\":2},"
                + "{\"category\":\"C\",\"name\":\"E2\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":2,\"rank\":3}]}";
        List<JsonNode> pages = runPipeline(json, 2, List.of()); // pageSize=2 → 2 pages
        assertEquals(2, pages.size(), "paging must still produce 2 pages with extra fields present");
        assertEquals(2, pages.get(0).path("search_engines").size(), "page 0 must have 2 items");
        assertEquals(1, pages.get(1).path("search_engines").size(), "page 1 must have 1 item");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // With array — explicit end-to-end scenarios
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void withArray_nestedSubArray_listPreservedInOutput() throws Exception {
        // list sub-array inside each search_engine must survive the full pipeline intact
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, List.of());
        JsonNode list = pages.get(0).path("search_engines").get(0).path("list");

        assertTrue(list.isArray(), "list must be a JSON array in output");
        assertEquals(2, list.size());
        assertEquals("url0", list.get(0).asText());
        assertEquals("url1", list.get(1).asText());
    }

    @Test
    void withArray_nestedObject_imdbPreservedInOutput() throws Exception {
        // imdb nested object must survive as a JSON object in output
        List<JsonNode> pages = runPipeline(ONE_ENGINE_JSON, 10, List.of());
        JsonNode imdb = pages.get(0).path("search_engines").get(0).path("imdb");

        assertTrue(imdb.isObject(), "imdb must be a JSON object in output");
        assertEquals("tt1",       imdb.path("id").asText());
        assertEquals("Some Film", imdb.path("title").asText());
    }

    @Test
    void withArray_multiplePagesWithArrays_eachPageContainsArray() throws Exception {
        // Every page must contain the search_engines array, even pages that are not the first
        List<JsonNode> pages = runPipeline(THREE_ENGINE_JSON, 2, List.of());
        for (int i = 0; i < pages.size(); i++) {
            assertTrue(pages.get(i).path("search_engines").isArray(),
                    "page " + i + " must contain the search_engines array");
        }
    }

    @Test
    void withArray_allDirectors_uppercasedAcrossAllPages() throws Exception {
        // All three directors spread across two pages must all be uppercased
        List<JsonNode> pages = runPipeline(THREE_ENGINE_JSON, 2, DIRECTOR_PATTERN);

        List<String> allDirectors = new ArrayList<>();
        for (JsonNode page : pages) {
            page.path("search_engines").forEach(engine ->
                    allDirectors.add(engine.path("imdb").path("director").asText()));
        }

        assertEquals(3, allDirectors.size(), "three directors total across all pages");
        allDirectors.forEach(d ->
                assertEquals(d.toUpperCase(java.util.Locale.ROOT), d,
                        "every director must be uppercased: " + d));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Without array — field absent, wrong type, or flat message
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void withoutArray_searchEnginesIsObject_routedToDlq() throws Exception {
        // search_engines is an object {…} instead of array […]
        String invalid = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":{\"name\":\"Engine\"}}";
        List<DlqRecord> dlq = runAndGetDlq(invalid);
        assertFalse(dlq.isEmpty(), "non-array search_engines must be routed to DLQ");
    }



    @Test
    void withoutArray_flatMessageNoArrayField_routedToDlq() throws Exception {
        // Completely flat message — no search_engines field at all
        String invalid = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"category\":\"C\",\"name\":\"Engine\"}";
        List<DlqRecord> dlq = runAndGetDlq(invalid);
        assertFalse(dlq.isEmpty(), "flat message without array field must be routed to DLQ");
    }

    @Test
    void withoutArray_itemsWithNoSubArraysOrObjects_outputIsFlatItems() throws Exception {
        // Items that have only scalar fields (no list, no imdb) — flat items
        String json = "{"
                + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":["
                + "{\"category\":\"Movies\",\"name\":\"Alpha\",\"pinned\":true,\"last_used\":1,\"list\":[]},"
                + "{\"category\":\"Series\",\"name\":\"Beta\",\"pinned\":false,\"last_used\":2,\"list\":[]}]}";
        List<JsonNode> pages = runPipeline(json, 10, List.of());

        assertEquals(1, pages.size());
        JsonNode engines = pages.get(0).path("search_engines");
        assertTrue(engines.isArray());
        assertEquals(2, engines.size());

        // Items must be objects with scalar fields only — no imdb sub-object
        assertFalse(engines.get(0).has("imdb"), "no imdb field expected");
        assertFalse(engines.get(1).has("imdb"), "no imdb field expected");
        assertEquals("Alpha", engines.get(0).path("name").asText());
        assertEquals("Beta",  engines.get(1).path("name").asText());
    }
}
