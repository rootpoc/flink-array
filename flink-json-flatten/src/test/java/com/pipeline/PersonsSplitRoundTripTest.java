package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.serialization.ReconstructSerializer;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip tests for a persons JSON payload through two pipeline configurations:
 *
 * <ol>
 *   <li><b>With split on {@code persons}</b> — the array is paginated; pagination
 *       metadata (index / total / count) is injected, so the reconstructed JSON
 *       differs from the input.</li>
 *   <li><b>Without split</b> — the entire document is flattened into a single Row;
 *       the reconstructed JSON is structurally identical to the input.</li>
 * </ol>
 *
 * <p>Schemas and input data are loaded from classpath resources:
 * <ul>
 *   <li>{@code schemas/input-persons-with-metadata.schema.json} — input schema
 *       (drives {@link InputSchemaInfo#analyze(JsonNode)})</li>
 *   <li>{@code schemas/output-persons-paged.schema.json} — output schema for
 *       split mode (drives {@link SchemaAnalyzer#analyze(JsonNode)} →
 *       {@link PaginationSchema})</li>
 *   <li>{@code input/persons-with-metadata.json} — the raw input message</li>
 * </ul>
 *
 * <p>Both tests: deserialize → inspect Row → serialize → compare JSON.
 */
class PersonsSplitRoundTripTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Loaded once per test class ────────────────────────────────────────────

    /** Raw bytes of the input JSON fixture loaded from classpath. */
    private static final byte[] PERSONS_BYTES;

    /** Parsed input JSON, used for structural comparison. */
    private static final JsonNode PERSONS_JSON_NODE;

    /** Input schema derived by analyzing {@code input-persons-with-metadata.schema.json}. */
    private static final InputSchemaInfo INPUT_SCHEMA;

    /**
     * Output pagination schema derived by analyzing {@code output-persons-paged.schema.json}.
     * Used only in test 1 (split mode).
     */
    private static final PaginationSchema OUTPUT_SCHEMA;

    static {
        try {
            PERSONS_BYTES     = loadResource("input/persons-with-metadata.json");
            PERSONS_JSON_NODE = MAPPER.readTree(PERSONS_BYTES);

            JsonNode inputSchemaNode  = MAPPER.readTree(
                    loadResource("schemas/input-persons-with-metadata.schema.json"));
            JsonNode outputSchemaNode = MAPPER.readTree(
                    loadResource("schemas/output-persons-paged.schema.json"));

            INPUT_SCHEMA  = InputSchemaInfo.analyze(inputSchemaNode);
            OUTPUT_SCHEMA = SchemaAnalyzer.analyze(outputSchemaNode);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private ReconstructSerializer serializer;

    @BeforeEach
    void setUp() throws Exception {
        serializer = new ReconstructSerializer("output-topic");
        serializer.open(null, null);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 1 — WITH split on "persons"
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Splits the {@code persons} array into pages of 1 item each, yielding two
     * output pages (one per person).
     *
     * <p>Steps:
     * <ol>
     *   <li>Derive {@link InputSchemaInfo} + {@link PaginationSchema} from classpath schemas.</li>
     *   <li>Deserialize + split via {@link ValidateSplitFlattenFunction} (pageSize = 1).</li>
     *   <li>Verify the Row for each page contains correct pagination metadata and person data.</li>
     *   <li>Serialize each page Row back to JSON.</li>
     *   <li>Assert that output JSON ≠ input JSON (pagination metadata injected; structure changed).</li>
     * </ol>
     *
     * <p><b>Note:</b> {@link ValidateSplitFlattenFunction} copies all non-array root fields
     * into each page Row, then overwrites the pagination fields with computed values.
     * Each page Row contains: {@code metadata.*} (with {@code metadata.page} = 0-based index,
     * {@code metadata.totalCount} = total pages) + {@code count} + {@code persons.0.*}.
     */
    @Test
    void withSplit_rowIsCorrect_andOutputJsonDiffersFromInput() throws Exception {
        System.out.println("\n=== Schema analysis ===");
        System.out.println("INPUT_SCHEMA  : " + INPUT_SCHEMA);
        System.out.println("OUTPUT_SCHEMA : " + OUTPUT_SCHEMA);

        // ── Stage 1: deserialize + split (pageSize=1 → 2 pages) ──────────────
        ValidateSplitFlattenFunction fn = new ValidateSplitFlattenFunction(
                INPUT_SCHEMA, OUTPUT_SCHEMA, 1, NullHandling.INCLUDE);

        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(
                ProcessedMessage.ofValue(PERSONS_BYTES), System.currentTimeMillis());
        List<Row> rows = harness.<ProcessedMessage>extractOutputValues()
                .stream()
                .map(ProcessedMessage::getPayload)
                .collect(Collectors.toList());
        harness.close();

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
        for (int i = 0; i < rows.size(); i++) {
            ProducerRecord<byte[], byte[]> record =
                    serializer.serialize(ProcessedMessage.ofPayload(rows.get(i)), null, 0L);
            assertNotNull(record, "serialize() must not return null for page " + i);

            JsonNode outputJson = MAPPER.readTree(record.value());
            System.out.println("Page " + i + " output JSON:\n" + outputJson.toPrettyString());

            // ── Assert output ≠ input ─────────────────────────────────────────
            assertNotEquals(PERSONS_JSON_NODE, outputJson,
                    "Page " + i + ": output JSON must differ from original input "
                    + "(persons array replaced by single-page slice, metadata.page updated).\n"
                    + "output: " + outputJson.toPrettyString());

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

    // ══════════════════════════════════════════════════════════════════════════
    // Test 2 — WITHOUT split
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Flattens the full JSON document without any splitting.
     *
     * <p>Steps:
     * <ol>
     *   <li>Derive {@link InputSchemaInfo} from the classpath input schema.</li>
     *   <li>Deserialize via {@link ValidateFlattenFunction} (no split).</li>
     *   <li>Verify the Row contains every leaf field from the original JSON.</li>
     *   <li>Serialize the Row back to JSON.</li>
     *   <li>Assert output JSON is structurally equal to the input JSON
     *       (pure flatten → reconstruct round-trip; no metadata injected).</li>
     * </ol>
     */
    @Test
    void withoutSplit_rowIsCorrect_andOutputJsonMatchesInput() throws Exception {
        // ── Stage 1: deserialize without split ───────────────────────────────
        ValidateFlattenFunction fn = new ValidateFlattenFunction(INPUT_SCHEMA, NullHandling.INCLUDE);

        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(
                ProcessedMessage.ofValue(PERSONS_BYTES), System.currentTimeMillis());
        List<Row> rows = harness.<ProcessedMessage>extractOutputValues()
                .stream()
                .map(ProcessedMessage::getPayload)
                .collect(Collectors.toList());
        harness.close();

        // ── Verify single Row (no split) ──────────────────────────────────────
        System.out.println("\n=== Test 2: WITHOUT split ===");
        System.out.println("Rows produced: " + rows.size());
        assertEquals(1, rows.size(), "no split → single row");

        Row row = rows.get(0);
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
        ProducerRecord<byte[], byte[]> record =
                serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);
        assertNotNull(record, "serialize() must not return null");

        JsonNode outputJson = MAPPER.readTree(record.value());
        System.out.println("Output JSON:\n" + outputJson.toPrettyString());

        // ── Assert output == input (structural equality) ───────────────────────
        assertEquals(PERSONS_JSON_NODE, outputJson,
                "No-split round-trip: reconstructed JSON must be structurally equal to input.\n"
                + "expected: " + PERSONS_JSON_NODE.toPrettyString() + "\n"
                + "actual:   " + outputJson.toPrettyString());

        System.out.println("=== Test 2 PASSED ===\n");
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
}







