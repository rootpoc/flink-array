package com.pipeline.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.InputProcessFunctionFactory;
import com.pipeline.OutputProcessFunctionFactory;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.SerializedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.config.PipelineConfig;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip tests for a persons JSON payload through the configured input and output
 * ProcessFunctions plus optional split paging.
 */
class PersonsSplitRoundTripTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Loaded once per test class ────────────────────────────────────────────

    /** Raw bytes of the input JSON fixture loaded from classpath. */
    private static final byte[] PERSONS_BYTES;

    /** Parsed input JSON, used for structural comparison. */
    private static final JsonNode PERSONS_JSON_NODE;

    /** Input schema derived by analyzing {@code input-persons-with-metadata.schema.json}. */
    private static final PipelineConfig JSON_CONFIG;

    /**
     * Output pagination schema derived by analyzing {@code output-persons-paged.schema.json}.
     * Used only in test 1 (split mode).
     */
    private static final PaginationSchema OUTPUT_SCHEMA;

    /** Raw bytes of the input JSON fixture (anyOf variant) loaded from classpath. */
    private static final byte[] PERSONS_ANYOF_BYTES;

    /** Parsed input JSON (anyOf variant), used for structural comparison. */
    private static final JsonNode PERSONS_ANYOF_JSON_NODE;

    /** Input schema (anyOf variant) derived by analyzing {@code input-persons-with-metadata.withAny.schema.json}. */
    private static final PipelineConfig JSON_ANYOF_CONFIG;

    /** Raw bytes of the flat JSON fixture loaded from classpath. */
    private static final byte[] FLAT_JSON_BYTES;

    /** Parsed flat JSON, used for structural comparison. */
    private static final JsonNode FLAT_JSON_NODE;

    /** Input schema derived by analyzing {@code csv-flat-person-required.schema.json}. */
    private static final PipelineConfig FLAT_JSON_CONFIG;

    /** Raw bytes of the complex JSON fixture loaded from classpath. */
    private static final byte[] COMPLEX_BYTES;

    /** Parsed complex JSON, used for structural comparison in the complex split test. */
    private static final JsonNode COMPLEX_JSON_NODE;

    /** Config that performs person paging during input processing for complex input. */
    private static final PipelineConfig COMPLEX_INPUT_SPLIT_CONFIG;

    static {
        try {
            PERSONS_BYTES     = loadResource("input/persons-with-metadata.json");
            PERSONS_JSON_NODE = MAPPER.readTree(PERSONS_BYTES);

            JSON_CONFIG = new PipelineConfig.Builder()
                    .inputFormat(PipelineConfig.InputFormat.JSON)
                    .outputFormat(PipelineConfig.OutputFormat.JSON)
                    .inputSchemaResource("schemas/input-persons-with-metadata.schema.json")
                    .outputSchemaResource("schemas/output-persons-paged.schema.json")
                    .splitField("persons")
                    .nullHandling(PipelineConfig.NullHandling.INCLUDE)
                    .build();

            JsonNode outputSchemaNode = MAPPER.readTree(
                    loadResource(JSON_CONFIG.getOutputSchemaResource()));
            OUTPUT_SCHEMA = SchemaAnalyzer.analyze(outputSchemaNode);

            PERSONS_ANYOF_BYTES     = loadResource("input/persons-with-metadata-anyof.json");
            PERSONS_ANYOF_JSON_NODE = MAPPER.readTree(PERSONS_ANYOF_BYTES);

            JSON_ANYOF_CONFIG = new PipelineConfig.Builder()
                    .inputFormat(PipelineConfig.InputFormat.JSON)
                    .outputFormat(PipelineConfig.OutputFormat.JSON)
                    .inputSchemaResource("schemas/input-persons-with-metadata.withAny.schema.json")
                    .outputSchemaResource("schemas/output-persons-paged.schema.json")
                    .splitField("persons")
                    .nullHandling(PipelineConfig.NullHandling.INCLUDE)
                    .build();

            FLAT_JSON_BYTES = loadResource("input/json-flat-parson.json");
            FLAT_JSON_NODE = MAPPER.readTree(FLAT_JSON_BYTES);

            FLAT_JSON_CONFIG = new PipelineConfig.Builder()
                    .inputFormat(PipelineConfig.InputFormat.JSON)
                    .outputFormat(PipelineConfig.OutputFormat.JSON)
                    .inputSchemaResource("schemas/csv-flat-person-required.schema.json")
                    .outputSchemaResource("schemas/csv-flat-person-required.schema.json")
                    .splitEnabled(false)
                    .nullHandling(PipelineConfig.NullHandling.INCLUDE)
                    .build();

            COMPLEX_BYTES = loadResource("input/complex2-paging.json");
            COMPLEX_JSON_NODE = MAPPER.readTree(COMPLEX_BYTES);

            COMPLEX_INPUT_SPLIT_CONFIG = new PipelineConfig.Builder()
                    .inputFormat(PipelineConfig.InputFormat.JSON)
                    .outputFormat(PipelineConfig.OutputFormat.JSON)
                    .inputSchemaResource("schemas/complex2.json")
                    .outputSchemaResource("schemas/output-persons-paged.schema.json")
                    .splittingPageSize(1)
                    .splitEnabled(true)
                    .splitStage(PipelineConfig.SplitStage.INPUT)
                    .splitField("persons")
                    .nullHandling(PipelineConfig.NullHandling.INCLUDE)
                    .build();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 1 — WITH split on "persons"
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Splits the {@code persons} array into pages of 1 item each, yielding two
     * output pages (one per person).
     */
    @Test
    void withSplit_rowIsCorrect_andOutputJsonDiffersFromInput() throws Exception {
        System.out.println("\n=== Schema analysis ===");
        System.out.println("INPUT_CONFIG  : " + JSON_CONFIG);
        System.out.println("OUTPUT_SCHEMA : " + OUTPUT_SCHEMA);

        // ── Stage 1: use the same config-driven ingress ProcessFunction as production
        List<ProcessedMessage> flattened = runInput(JSON_CONFIG, PERSONS_BYTES);
        assertEquals(1, flattened.size(), "input process function must emit exactly one full Row");

        // ── Stage 2: split paged output from the flattened Row ────────────────
        var splitHarness = new OneInputStreamOperatorTestHarness<>(
                new org.apache.flink.streaming.api.operators.StreamFlatMap<>(
                        new com.pipeline.splitting.SplitFunction(OUTPUT_SCHEMA, 1, JSON_CONFIG.getSplitField())));
        splitHarness.setup(ProcessedMessageSerializer.INSTANCE);
        splitHarness.open();
        for (ProcessedMessage msg : flattened) {
            splitHarness.processElement(msg, System.currentTimeMillis());
        }
        List<ProcessedMessage> pageMessages = splitHarness.extractOutputValues();
        List<Row> rows = pageMessages.stream().map(ProcessedMessage::getPayload).collect(Collectors.toList());
        splitHarness.close();

        // ── Verify 2 pages were produced ──────────────────────────────────────
        System.out.println("\n=== Test 1: WITH split on 'persons' (pageSize=1) ===");
        System.out.println("Pages produced : " + rows.size());
        assertEquals(2, rows.size(), "pageSize=1 with 2 persons → 2 pages");

        // ── Inspect page 0 (John Doe) ─────────────────────────────────────────
        Row page0 = rows.get(0);
        printRow("Page 0", page0);

        // Pagination fields — index and total are nested inside metadata.*
        // OUTPUT_SCHEMA.getIndexFieldName() = "metadata.page"   (0-based, overrides input)
        // OUTPUT_SCHEMA.getTotalFieldName()  = "metadata.totalCount" (total pages, overrides input)
        // OUTPUT_SCHEMA.getCountFieldName()  = "count"           (items in this page)
        assertEquals(0,  page0.getField(OUTPUT_SCHEMA.getIndexFieldName()), "page 0: metadata.page");
        assertEquals(2,  page0.getField(OUTPUT_SCHEMA.getTotalFieldName()), "page 0: metadata.totalCount");
        assertEquals(1,  page0.getField(OUTPUT_SCHEMA.getCountFieldName()), "page 0: count");

        // Passthrough metadata fields — copied from input root
        assertEquals("2026-03-14T17:57:00Z", page0.getField("metadata.timestamp"));
        assertEquals("1.0",                  page0.getField("metadata.apiVersion"));
        assertEquals(10L,                    page0.getField("metadata.pageSize"));

        // Person 0 (John Doe) — re-indexed to 0 within the page.
        String arr = OUTPUT_SCHEMA.getArrayFieldName();
        assertEquals("John",        page0.getField(arr + ".0.firstName"));
        assertEquals("Doe",         page0.getField(arr + ".0.lastName"));
        assertEquals(35L,            page0.getField(arr + ".0.age"));
        assertEquals("123 Main St", page0.getField(arr + ".0.address.street"));
        assertEquals("Anytown",     page0.getField(arr + ".0.address.city"));
        assertEquals("12345",       page0.getField(arr + ".0.address.postalCode"));
        assertEquals("USA",         page0.getField(arr + ".0.address.country"));

        // ── Inspect page 1 (Jane Smith) ───────────────────────────────────────
        Row page1 = rows.get(1);
        printRow("Page 1", page1);

        assertEquals(1,  page1.getField(OUTPUT_SCHEMA.getIndexFieldName()), "page 1: metadata.page");
        assertEquals(2,  page1.getField(OUTPUT_SCHEMA.getTotalFieldName()), "page 1: metadata.totalCount");
        assertEquals(1,  page1.getField(OUTPUT_SCHEMA.getCountFieldName()), "page 1: count");

        assertEquals("2026-03-14T17:57:00Z", page1.getField("metadata.timestamp"));

        assertEquals("Jane",        page1.getField(arr + ".0.firstName"));
        assertEquals("Smith",       page1.getField(arr + ".0.lastName"));
        assertEquals(28L,            page1.getField(arr + ".0.age"));
        assertEquals("456 Oak Ave", page1.getField(arr + ".0.address.street"));
        assertEquals("Othertown",   page1.getField(arr + ".0.address.city"));
        assertEquals("67890",       page1.getField(arr + ".0.address.postalCode"));
        assertEquals("USA",         page1.getField(arr + ".0.address.country"));

        // ── Stage 2: serialize each page Row → JSON ───────────────────────────
        for (ProcessedMessage pageMsg : pageMessages) {
            byte[] bytes = runOutput(JSON_CONFIG, pageMsg).getValue();
            JsonNode outputJson = MAPPER.readTree(bytes);
            System.out.println("Output JSON:\n" + outputJson.toPrettyString());

            // ── Assert output ≠ input ─────────────────────────────────────────
            assertNotEquals(PERSONS_JSON_NODE, outputJson,
                    "Split output JSON must differ from original input.");

            // ── metadata must be present and contain pagination fields ─────────
            assertTrue(outputJson.has("metadata"),
                    "output must have 'metadata'");
            assertTrue(outputJson.path("metadata").has("page"),
                    "output metadata must have 'page' (index)");
            assertTrue(outputJson.path("metadata").has("totalCount"),
                    "output metadata must have 'totalCount' (total pages)");
            assertTrue(outputJson.path("metadata").has("timestamp"),
                    "output metadata must have 'timestamp'");

            // ── top-level count must be present ───────────────────────────────
            assertTrue(outputJson.has(OUTPUT_SCHEMA.getCountFieldName()),
                    "output must have '" + OUTPUT_SCHEMA.getCountFieldName() + "'");

            // ── input does NOT have a top-level count field ───────────────────
            assertFalse(PERSONS_JSON_NODE.has(OUTPUT_SCHEMA.getCountFieldName()),
                    "input must NOT have '" + OUTPUT_SCHEMA.getCountFieldName() + "'");
        }

        System.out.println("=== Test 1 PASSED ===\n");
    }
    /**
     * Splits the {@code persons} array into pages of 1 item each, yielding two
     * output pages (one per person).
     */
    @Test
    void withSplit_rowIsCorrect_complexInput() throws Exception {
        System.out.println("\n=== Complex input schema analysis ===");
        System.out.println("INPUT_CONFIG  : " + COMPLEX_INPUT_SPLIT_CONFIG);
        System.out.println("OUTPUT_SCHEMA : " + OUTPUT_SCHEMA);

        List<ProcessedMessage> pageMessages = runInput(COMPLEX_INPUT_SPLIT_CONFIG, COMPLEX_BYTES);
        List<Row> rows = pageMessages.stream().map(ProcessedMessage::getPayload).collect(Collectors.toList());

        System.out.println("\n=== Complex input split during input processing (pageSize=1) ===");
        System.out.println("Pages produced : " + rows.size());
        assertEquals(2, rows.size(), "input-stage splitting must emit one message per person");

        Row page0 = rows.get(0);
        printRow("Complex page 0", page0);
        assertEquals(0, page0.getField(OUTPUT_SCHEMA.getIndexFieldName()), "page 0: metadata.page");
        assertEquals(2, page0.getField(OUTPUT_SCHEMA.getTotalFieldName()), "page 0: metadata.totalCount");
        assertEquals(1, page0.getField(OUTPUT_SCHEMA.getCountFieldName()), "page 0: count");
        assertEquals("2026-03-14T17:57:00Z", page0.getField("metadata.timestamp"));
        assertEquals("John", page0.getField("persons.0.firstName"));
        assertEquals("Doe", page0.getField("persons.0.lastName"));
        assertEquals(35L, page0.getField("persons.0.age"));
        assertEquals(91L, page0.getField("persons.0.grades.0.school.mathGarde"));
        assertEquals(82L, page0.getField("persons.0.grades.0.highschool.historyGrade"));
        assertEquals(96L, page0.getField("persons.0.grades.1.mathGarde"));
        assertEquals(86L, page0.getField("persons.0.grades.1.historyGrade"));
        assertNull(page0.getField("persons.1.firstName"), "page 0 must not contain the next person after input-stage split");

        Row page1 = rows.get(1);
        printRow("Complex page 1", page1);
        assertEquals(1, page1.getField(OUTPUT_SCHEMA.getIndexFieldName()), "page 1: metadata.page");
        assertEquals(2, page1.getField(OUTPUT_SCHEMA.getTotalFieldName()), "page 1: metadata.totalCount");
        assertEquals(1, page1.getField(OUTPUT_SCHEMA.getCountFieldName()), "page 1: count");
        assertEquals("2026-03-14T17:57:00Z", page1.getField("metadata.timestamp"));
        assertEquals("Jane", page1.getField("persons.0.firstName"));
        assertEquals("Smith", page1.getField("persons.0.lastName"));
        assertEquals(31L, page1.getField("persons.0.age"));
        assertEquals(71L, page1.getField("persons.0.grades.0.school.mathGarde"));
        assertEquals(62L, page1.getField("persons.0.grades.0.highschool.historyGrade"));
        assertEquals(76L, page1.getField("persons.0.grades.1.mathGarde"));
        assertEquals(66L, page1.getField("persons.0.grades.1.historyGrade"));

        assertEquals(2, COMPLEX_JSON_NODE.path("persons").size(), "complex fixture should keep two persons at the source");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 2 — WITHOUT split
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Flattens the full JSON document without any splitting and verifies that the
     * configured output process recreates the original JSON.
     */
    @Test
    void withoutSplit_rowIsCorrect_andOutputJsonMatchesInput() throws Exception {
        List<ProcessedMessage> messages = runInput(JSON_CONFIG, PERSONS_BYTES);
        assertEquals(1, messages.size(), "no split → one message");
        Row row = messages.get(0).getPayload();

        System.out.println("\n=== Test 2: WITHOUT split ===");
        printRow("Full row (no split)", row);

        // ── Verify metadata leaf fields ────────────────────────────────────────
        // extractLeafValue always returns Long for integral numbers (ternary type promotion)
        assertEquals(2L,                     row.getField("metadata.totalCount"));
        assertEquals(1L,                     row.getField("metadata.page"));
        assertEquals(10L,                    row.getField("metadata.pageSize"));
        assertEquals("2026-03-14T17:57:00Z", row.getField("metadata.timestamp"));
        assertEquals("1.0",                  row.getField("metadata.apiVersion"));

        // ── Verify Person 0 — John Doe ────────────────────────────────────────
        assertEquals("John",        row.getField("persons.0.firstName"));
        assertEquals("Doe",         row.getField("persons.0.lastName"));
        assertEquals(35L,            row.getField("persons.0.age"));
        assertEquals("123 Main St", row.getField("persons.0.address.street"));
        assertEquals("Anytown",     row.getField("persons.0.address.city"));
        assertEquals("12345",       row.getField("persons.0.address.postalCode"));
        assertEquals("USA",         row.getField("persons.0.address.country"));

        // ── Verify Person 1 — Jane Smith ──────────────────────────────────────
        assertEquals("Jane",        row.getField("persons.1.firstName"));
        assertEquals("Smith",       row.getField("persons.1.lastName"));
        assertEquals(28L,            row.getField("persons.1.age"));
        assertEquals("456 Oak Ave", row.getField("persons.1.address.street"));
        assertEquals("Othertown",   row.getField("persons.1.address.city"));
        assertEquals("67890",       row.getField("persons.1.address.postalCode"));
        assertEquals("USA",         row.getField("persons.1.address.country"));

        // ── No pagination fields ──────────────────────────────────────────────
        Set<String> fieldNames = row.getFieldNames(false);
        assertNotNull(fieldNames);
        assertFalse(fieldNames.contains("index"), "no-split row must not contain 'index'");
        assertFalse(fieldNames.contains("total"), "no-split row must not contain 'total'");
        assertFalse(fieldNames.contains("count"), "no-split row must not contain 'count'");

        // ── Verify total field count ───────────────────────────────────────────
        // 5 metadata fields + 2 persons × 7 fields (firstName, lastName, age,
        // address.street, address.city, address.postalCode, address.country) = 19
        System.out.println("Total fields in row: " + fieldNames.size());
        assertEquals(19, fieldNames.size(),
                "expected 5 metadata + 2 × 7 person fields = 19 leaf fields.\n"
                + "Actual fields: " + fieldNames.stream().sorted().collect(Collectors.toList()));

        // ── Stage 2: serialize Row → JSON ──────────────────────────────────────
        byte[] bytes = runOutput(JSON_CONFIG, messages.get(0)).getValue();
        JsonNode outputJson = MAPPER.readTree(bytes);
        System.out.println("Output JSON:\n" + outputJson.toPrettyString());

        // ── Assert output == input (structural equality) ───────────────────────
        assertEquals(PERSONS_JSON_NODE, outputJson,
                "No-split round-trip: reconstructed JSON must be structurally equal to input.\n"
                + "expected: " + PERSONS_JSON_NODE.toPrettyString() + "\n"
                + "actual:   " + outputJson.toPrettyString());

        System.out.println("=== Test 2 PASSED ===\n");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 3 — WITH split on "persons" (anyOf variant)
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Splits the {@code persons} array into pages of 1 item each, yielding two
     * output pages (one per person). This test verifies the behavior when the
     * input JSON uses an anyOf schema and a required field (lastName) is missing
     * from the first object.
     */
    @Test
    void withSplit_anyOfInput_missingLastNameAccepted_andOutputJsonDiffersFromInput() throws Exception {
        System.out.println("\n=== anyOf schema analysis ===");
        System.out.println("INPUT_CONFIG  : " + JSON_ANYOF_CONFIG);
        System.out.println("OUTPUT_SCHEMA : " + OUTPUT_SCHEMA);

        List<ProcessedMessage> flattened = runInput(JSON_ANYOF_CONFIG, PERSONS_ANYOF_BYTES);
        assertEquals(1, flattened.size(), "input process function must emit exactly one full Row for anyOf input");

        var splitHarness = new OneInputStreamOperatorTestHarness<>(
                new org.apache.flink.streaming.api.operators.StreamFlatMap<>(
                        new com.pipeline.splitting.SplitFunction(OUTPUT_SCHEMA, 1, JSON_ANYOF_CONFIG.getSplitField())));
        splitHarness.setup(ProcessedMessageSerializer.INSTANCE);
        splitHarness.open();
        for (ProcessedMessage msg : flattened) {
            splitHarness.processElement(msg, System.currentTimeMillis());
        }
        List<ProcessedMessage> pageMessages = splitHarness.extractOutputValues();
        List<Row> rows = pageMessages.stream().map(ProcessedMessage::getPayload).collect(Collectors.toList());
        splitHarness.close();

        // ── Verify 2 pages were produced ──────────────────────────────────────
        assertEquals(2, rows.size(), "pageSize=1 with 2 persons → 2 pages");

        // ── Inspect page 0 (John Doe) ─────────────────────────────────────────
        Row page0 = rows.get(0);
        printRow("anyOf Page 0", page0);
        assertEquals(0, page0.getField(OUTPUT_SCHEMA.getIndexFieldName()));
        assertEquals(2, page0.getField(OUTPUT_SCHEMA.getTotalFieldName()));
        assertEquals(1, page0.getField(OUTPUT_SCHEMA.getCountFieldName()));
        assertEquals("John", page0.getField("persons.0.firstName"));
        assertNull(page0.getField("persons.0.lastName"), "page 0 lastName must be absent/null under anyOf");
        Set<String> page0Names = page0.getFieldNames(false);
        assertNotNull(page0Names);
        assertFalse(page0Names.contains("persons.0.lastName"), "page 0 must not contain persons.0.lastName field");

        // ── Inspect page 1 (Jane Smith) ───────────────────────────────────────
        Row page1 = rows.get(1);
        printRow("anyOf Page 1", page1);
        assertEquals(1, page1.getField(OUTPUT_SCHEMA.getIndexFieldName()));
        assertEquals(2, page1.getField(OUTPUT_SCHEMA.getTotalFieldName()));
        assertEquals(1, page1.getField(OUTPUT_SCHEMA.getCountFieldName()));
        assertEquals("Jane",  page1.getField("persons.0.firstName"));
        assertEquals("Smith", page1.getField("persons.0.lastName"));

        // ── Stage 2: serialize each page Row → JSON ───────────────────────────
        for (ProcessedMessage pageMsg : pageMessages) {
            byte[] bytes = runOutput(JSON_ANYOF_CONFIG, pageMsg).getValue();
            JsonNode outputJson = MAPPER.readTree(bytes);
            System.out.println("anyOf Output JSON:\n" + outputJson.toPrettyString());

            // ── Assert output ≠ input ─────────────────────────────────────────
            assertNotEquals(PERSONS_ANYOF_JSON_NODE, outputJson,
                    "Split output JSON must differ from original anyOf input.");

            // ── metadata must be present and contain pagination fields ─────────
            assertTrue(outputJson.has("metadata"),
                    "output must have 'metadata'");
            assertTrue(outputJson.path("metadata").has("page"),
                    "output metadata must have 'page' (index)");
            assertTrue(outputJson.path("metadata").has("totalCount"),
                    "output metadata must have 'totalCount' (total pages)");
            assertTrue(outputJson.path("metadata").has("timestamp"),
                    "output metadata must have 'timestamp'");

            // ── top-level count must be present ───────────────────────────────
            assertTrue(outputJson.has(OUTPUT_SCHEMA.getCountFieldName()),
                    "output must have '" + OUTPUT_SCHEMA.getCountFieldName() + "'");

            // ── input does NOT have a top-level count field ───────────────────
            assertFalse(PERSONS_ANYOF_JSON_NODE.has(OUTPUT_SCHEMA.getCountFieldName()),
                    "input must NOT have '" + OUTPUT_SCHEMA.getCountFieldName() + "'");
        }

        System.out.println("=== Test 3 PASSED ===\n");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 4 — WITHOUT split (anyOf variant)
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Flattens the full JSON document without any splitting and verifies that the
     * configured output process recreates the original JSON. This test verifies the
     * behavior when the input JSON uses an anyOf schema and a required field (lastName)
     * is missing from the first object.
     */
    @Test
    void withoutSplit_anyOfInput_rowIsCorrect_andOutputJsonMatchesInput() throws Exception {
        List<ProcessedMessage> messages = runInput(JSON_ANYOF_CONFIG, PERSONS_ANYOF_BYTES);
        assertEquals(1, messages.size(), "no split → one message");
        Row row = messages.get(0).getPayload();

        System.out.println("\n=== Test anyOf WITHOUT split ===");
        printRow("Full row (anyOf no split)", row);

        assertEquals(2L,                     row.getField("metadata.totalCount"));
        assertEquals(1L,                     row.getField("metadata.page"));
        assertEquals(10L,                    row.getField("metadata.pageSize"));
        assertEquals("2026-03-14T17:57:00Z", row.getField("metadata.timestamp"));
        assertEquals("1.0",                  row.getField("metadata.apiVersion"));

        assertEquals("John",        row.getField("persons.0.firstName"));
        assertNull(row.getField("persons.0.lastName"), "persons.0.lastName must be absent/null under anyOf");
        assertEquals(35L,            row.getField("persons.0.age"));
        assertEquals("123 Main St", row.getField("persons.0.address.street"));
        assertEquals("Anytown",     row.getField("persons.0.address.city"));
        assertEquals("12345",       row.getField("persons.0.address.postalCode"));
        assertEquals("USA",         row.getField("persons.0.address.country"));

        assertEquals("Jane",        row.getField("persons.1.firstName"));
        assertEquals("Smith",       row.getField("persons.1.lastName"));
        assertEquals(28L,            row.getField("persons.1.age"));
        assertEquals("456 Oak Ave", row.getField("persons.1.address.street"));
        assertEquals("Othertown",   row.getField("persons.1.address.city"));
        assertEquals("67890",       row.getField("persons.1.address.postalCode"));
        assertEquals("USA",         row.getField("persons.1.address.country"));

        Set<String> fieldNames = row.getFieldNames(false);
        assertNotNull(fieldNames);
        assertFalse(fieldNames.contains("persons.0.lastName"), "row must not contain persons.0.lastName field");
        assertEquals(18, fieldNames.size(),
                "expected 5 metadata + 13 person leaf fields because persons.0.lastName is absent.\n"
                + "Actual fields: " + fieldNames.stream().sorted().collect(Collectors.toList()));

        byte[] bytes = runOutput(JSON_ANYOF_CONFIG, messages.get(0)).getValue();
        JsonNode outputJson = MAPPER.readTree(bytes);
        System.out.println("anyOf Output JSON:\n" + outputJson.toPrettyString());
        assertEquals(PERSONS_ANYOF_JSON_NODE, outputJson,
                "No-split anyOf round-trip: reconstructed JSON must equal input.");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 5 — WITHOUT split (flat JSON)
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Flattens the full JSON document without any splitting and verifies that the
     * configured output process recreates the original JSON. This test uses a flat
     * JSON schema and data.
     */
    @Test
    void withoutSplit_flatJson_rowIsCorrect_andOutputJsonMatchesInput() throws Exception {
        List<ProcessedMessage> messages = runInput(FLAT_JSON_CONFIG, FLAT_JSON_BYTES);
        assertEquals(1, messages.size(), "flat json -> one message");
        Row row = messages.get(0).getPayload();

        System.out.println("\n=== Test flat JSON WITHOUT split ===");
        printRow("Flat json row", row);

        assertEquals("John",        row.getField("firstName"));
        assertEquals("Doe",         row.getField("lastName"));
        assertEquals(35L,            row.getField("age"));
        assertEquals("123 Main St", row.getField("street"));
        assertEquals("Anytown",     row.getField("city"));
        assertEquals("12345",       row.getField("postalCode"));
        assertEquals("USA",         row.getField("country"));

        Set<String> fieldNames = row.getFieldNames(false);
        assertNotNull(fieldNames);
        assertEquals(7, fieldNames.size(), "flat object must have exactly 7 leaf fields");
        assertFalse(fieldNames.contains("count"), "flat row must not contain count");
        assertFalse(fieldNames.contains("index"), "flat row must not contain index");
        assertFalse(fieldNames.contains("total"), "flat row must not contain total");

        byte[] bytes = runOutput(FLAT_JSON_CONFIG, messages.get(0)).getValue();
        JsonNode outputJson = MAPPER.readTree(bytes);
        System.out.println("Flat output JSON:\n" + outputJson.toPrettyString());
        assertEquals(FLAT_JSON_NODE, outputJson,
                "Flat JSON round-trip must reconstruct the original input exactly.");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Loads a classpath resource (from src/test/resources) as a byte array. */
    private static byte[] loadResource(String path) throws Exception {
        try (InputStream is = PersonsSplitRoundTripTest.class
                .getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Classpath resource not found: " + path);
            return is.readAllBytes();
        }
    }

    private static void printRow(String label, Row row) {
        Set<String> names = row.getFieldNames(false);
        if (names == null) {
            System.out.println("[" + label + "] (positional row — no field names)");
            return;
        }
        System.out.println("\n--- " + label + " (" + names.size() + " fields) ---");
        names.stream().sorted().forEach(name ->
                System.out.printf("  %-50s = %s%n", name, row.getField(name)));
        System.out.println("---");
    }

    private static List<ProcessedMessage> runInput(PipelineConfig config, byte[] bytes) throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(InputProcessFunctionFactory.create(config)));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(bytes), System.currentTimeMillis());
        List<ProcessedMessage> output = harness.extractOutputValues();
        harness.close();
        return output;
    }

    private static SerializedMessage runOutput(PipelineConfig config, ProcessedMessage msg) throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(OutputProcessFunctionFactory.create(config)));
        harness.setup(org.apache.flink.api.common.typeinfo.TypeInformation.of(SerializedMessage.class)
                .createSerializer(new org.apache.flink.api.common.ExecutionConfig()));
        harness.open();
        harness.processElement(msg, System.currentTimeMillis());
        List<SerializedMessage> output = harness.extractOutputValues();
        harness.close();
        assertEquals(1, output.size(), "output process function must emit exactly one serialized message");
        return output.get(0);
    }

    /**
     * Verifies that a no-split JSON message missing a required person field is
     * rejected during input validation and routed to the DLQ.
     */
    @Test
    void withoutSplit_rowIsInCorrect_whenInputJsonIsMissingRequiredSchemaField() throws Exception {
        ObjectNode invalidJson = PERSONS_JSON_NODE.deepCopy();
        ((ObjectNode) invalidJson.get("persons").get(0)).remove("firstName");
        byte[] invalidBytes = MAPPER.writeValueAsBytes(invalidJson);

        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(InputProcessFunctionFactory.create(JSON_CONFIG)));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(invalidBytes), System.currentTimeMillis());

        assertTrue(harness.extractOutputValues().isEmpty(),
                "input missing a required schema field must not emit a Row");

        List<DlqRecord> dlqRecords = harness.getSideOutput(InputProcessFunctionFactory.DLQ_TAG).stream()
                .map(org.apache.flink.streaming.runtime.streamrecord.StreamRecord::getValue)
                .collect(Collectors.toList());
        assertEquals(1, dlqRecords.size(), "invalid JSON must produce exactly one DLQ record");

        DlqRecord dlq = dlqRecords.get(0);
        assertArrayEquals(invalidBytes, dlq.getOriginalBytes(),
                "DLQ record must preserve the invalid input payload");
        assertEquals(IllegalArgumentException.class.getName(), dlq.getErrorClass());
        assertTrue(dlq.getErrorMessage().contains("Input schema validation failed"),
                "DLQ error must mention schema validation failure: " + dlq.getErrorMessage());
        assertTrue(dlq.getErrorMessage().contains("missing required field 'persons.0.firstName'"),
                "DLQ error must identify the missing field: " + dlq.getErrorMessage());

        harness.close();
    }
}
