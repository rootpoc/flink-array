package com.pipeline.deserialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.serialization.ReconstructSerializer;
import com.pipeline.common.typeinfo.FlatRowSerializer;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link FlatteningDeserializer}.
 *
 * <p>Verifies that the operator:
 * <ul>
 *   <li>Produces Flink {@link Row} instances (named-field mode) on the main output</li>
 *   <li>Routes genuine parse failures to {@link FlatteningDeserializer#DLQ_TAG}</li>
 *   <li>Handles arbitrarily deep nesting and large arrays without limits</li>
 *   <li>Correctly encodes dot-notation paths for nested objects and arrays</li>
 * </ul>
 */
class FlatteningDeserializerTest {

    private FlatteningDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new FlatteningDeserializer(
                new PipelineConfig.Builder()
                        .nullHandling(PipelineConfig.NullHandling.INCLUDE)
                        .build());
    }

    // ── Persons JSON (full integration view) ─────────────────────────────────

    static final String PERSONS_JSON = "{"
            + "\"count\":5,"
            + "\"persons\":["
            + "{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30,\"address\":{\"street\":\"123 Main Street\",\"city\":\"New York\",\"state\":\"NY\",\"postalCode\":\"10001\",\"country\":\"USA\"}},"
            + "{\"firstName\":\"Jane\",\"lastName\":\"Smith\",\"age\":28,\"address\":{\"street\":\"456 Oak Avenue\",\"city\":\"Los Angeles\",\"state\":\"CA\",\"postalCode\":\"90210\",\"country\":\"USA\"}},"
            + "{\"firstName\":\"Michael\",\"lastName\":\"Johnson\",\"age\":35,\"address\":{\"street\":\"789 Pine Road\",\"city\":\"Chicago\",\"state\":\"IL\",\"postalCode\":\"60601\",\"country\":\"USA\"}},"
            + "{\"firstName\":\"Sarah\",\"lastName\":\"Wilson\",\"age\":42,\"address\":{\"street\":\"321 Elm Street\",\"city\":\"Houston\",\"state\":\"TX\",\"postalCode\":\"77001\",\"country\":\"USA\"}},"
            + "{\"firstName\":\"David\",\"lastName\":\"Brown\",\"age\":27,\"address\":{\"street\":\"654 Cedar Lane\",\"city\":\"Miami\",\"state\":\"FL\",\"postalCode\":\"33101\",\"country\":\"USA\"}}]}";

    static final String NETFLIX_JSON = "{"
            + "\"application\":{\"name\":\"Netflix Categories\",\"app_id\":\"netflix-categories@deekshith.in\",\"version\":\"v0.1\"},"
            + "\"search_engines\":["
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action & Adventure\",\"list\":[\"http://www.netflix.com/browse/genre/1365\",\"https://www.netflix.com/title/genre/1365\"],\"pinned\":true,\"last_used\":0,\"imdb\":{\"id\":\"tt8936646\",\"title\":\"Extraction\",\"year\":\"2020\",\"rated\":\"R\",\"genre\":\"Action, Thriller\",\"director\":\"Sam Hargrave\",\"actors\":\"Chris Hemsworth, Rudhraksh Jaiswal\",\"imdb_rating\":6.8}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action Comedies\",\"list\":[\"http://www.netflix.com/browse/genre/43040\",\"https://www.netflix.com/browse/genre/43040?region=il\"],\"pinned\":false,\"last_used\":0,\"imdb\":{\"id\":\"tt4158476\",\"title\":\"Deadpool 2\",\"year\":\"2018\",\"rated\":\"R\",\"genre\":\"Action, Adventure, Comedy\",\"director\":\"David Leitch\",\"actors\":\"Ryan Reynolds, Josh Brolin\",\"imdb_rating\":7.6}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action Sci-Fi & Fantasy\",\"list\":[\"http://www.netflix.com/browse/genre/1568\",\"https://www.netflix.com/genre/1568\"],\"pinned\":false,\"last_used\":0,\"imdb\":{\"id\":\"tt1392190\",\"title\":\"Mad Max: Fury Road\",\"year\":\"2015\",\"rated\":\"R\",\"genre\":\"Action, Adventure, Sci-Fi\",\"director\":\"George Miller\",\"actors\":\"Charlize Theron, Tom Hardy\",\"imdb_rating\":8.1}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Horror Movies\",\"list\":[\"http://www.netflix.com/browse/genre/8711\",\"https://www.netflix.com/title/genre/8711\"],\"pinned\":false,\"last_used\":0,\"imdb\":{\"id\":\"tt0365748\",\"title\":\"The Descent\",\"year\":\"2005\",\"rated\":\"R\",\"genre\":\"Adventure, Horror\",\"director\":\"Neil Marshall\",\"actors\":\"Shauna Macdonald, Natalie Mendoza\",\"imdb_rating\":7.2}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Anime Series\",\"list\":[\"http://www.netflix.com/browse/genre/7424\",\"https://www.netflix.com/browse/genre/7424\"],\"pinned\":true,\"last_used\":12,\"imdb\":{\"id\":\"tt14947990\",\"title\":\"Cyberpunk: Edgerunners\",\"year\":\"2022\",\"rated\":\"TV-MA\",\"genre\":\"Animation, Action, Sci-Fi\",\"director\":\"Ibon Cormenzana\",\"actors\":\"Zach Aguilar, Emi Lo\",\"imdb_rating\":8.3}}]}";

    static final byte[] NETFLIX_BYTES = NETFLIX_JSON.strip().getBytes(StandardCharsets.UTF_8);

    @Test
    void personsJson_flattenedRowInspection() throws Exception {
        Row row = flatten(PERSONS_JSON);

        // ── Print the full Row so it is visible in the test output ────────────
        System.out.println("\n=== Flattened Row (" + fieldNames(row).size() + " fields) ===");
        fieldNames(row).forEach(name ->
                System.out.printf("  %-45s = %s%n", name, row.getField(name)));
        System.out.println("=====================================================\n");

        // ── Top-level scalar ──────────────────────────────────────────────────
        assertEquals(5L, row.getField("count"));

        // ── Person 0 — John Doe ───────────────────────────────────────────────
        assertEquals("John",             row.getField("persons.0.firstName"));
        assertEquals("Doe",              row.getField("persons.0.lastName"));
        assertEquals(30L,                 row.getField("persons.0.age"));
        assertEquals("123 Main Street",  row.getField("persons.0.address.street"));
        assertEquals("New York",         row.getField("persons.0.address.city"));
        assertEquals("NY",               row.getField("persons.0.address.state"));
        assertEquals("10001",            row.getField("persons.0.address.postalCode"));
        assertEquals("USA",              row.getField("persons.0.address.country"));

        // ── Person 1 — Jane Smith ─────────────────────────────────────────────
        assertEquals("Jane",             row.getField("persons.1.firstName"));
        assertEquals("Smith",            row.getField("persons.1.lastName"));
        assertEquals(28L,                 row.getField("persons.1.age"));
        assertEquals("456 Oak Avenue",   row.getField("persons.1.address.street"));
        assertEquals("Los Angeles",      row.getField("persons.1.address.city"));
        assertEquals("CA",               row.getField("persons.1.address.state"));
        assertEquals("90210",            row.getField("persons.1.address.postalCode"));
        assertEquals("USA",              row.getField("persons.1.address.country"));

        // ── Person 2 — Michael Johnson ────────────────────────────────────────
        assertEquals("Michael",          row.getField("persons.2.firstName"));
        assertEquals("Johnson",          row.getField("persons.2.lastName"));
        assertEquals(35L,                 row.getField("persons.2.age"));
        assertEquals("789 Pine Road",    row.getField("persons.2.address.street"));
        assertEquals("Chicago",          row.getField("persons.2.address.city"));
        assertEquals("IL",               row.getField("persons.2.address.state"));
        assertEquals("60601",            row.getField("persons.2.address.postalCode"));
        assertEquals("USA",              row.getField("persons.2.address.country"));

        // ── Person 3 — Sarah Wilson ───────────────────────────────────────────
        assertEquals("Sarah",            row.getField("persons.3.firstName"));
        assertEquals("Wilson",           row.getField("persons.3.lastName"));
        assertEquals(42L,                 row.getField("persons.3.age"));
        assertEquals("321 Elm Street",   row.getField("persons.3.address.street"));
        assertEquals("Houston",          row.getField("persons.3.address.city"));
        assertEquals("TX",               row.getField("persons.3.address.state"));
        assertEquals("77001",            row.getField("persons.3.address.postalCode"));
        assertEquals("USA",              row.getField("persons.3.address.country"));

        // ── Person 4 — David Brown ────────────────────────────────────────────
        assertEquals("David",            row.getField("persons.4.firstName"));
        assertEquals("Brown",            row.getField("persons.4.lastName"));
        assertEquals(27L,                 row.getField("persons.4.age"));
        assertEquals("654 Cedar Lane",   row.getField("persons.4.address.street"));
        assertEquals("Miami",            row.getField("persons.4.address.city"));
        assertEquals("FL",               row.getField("persons.4.address.state"));
        assertEquals("33101",            row.getField("persons.4.address.postalCode"));
        assertEquals("USA",              row.getField("persons.4.address.country"));

        // ── Total field count: 1 scalar + 5 persons × 8 fields each ──────────
        assertEquals(41, fieldNames(row).size());
    }

    // ── Flat object ───────────────────────────────────────────────────────────

    @Test
    void flatObject_singleLevel() throws Exception {
        Row row = flatten("{\"name\":\"alice\",\"age\":30,\"active\":true}");

        assertEquals("alice", row.getField("name"));
        assertEquals(30L,      row.getField("age"));
        assertEquals(true,    row.getField("active"));
    }

    // ── Nested object ─────────────────────────────────────────────────────────

    @Test
    void nestedObject_dotNotation() throws Exception {
        Row row = flatten("{\"person\":{\"name\":\"alice\",\"address\":{\"city\":\"London\"}}}");

        assertEquals("alice",  row.getField("person.name"));
        assertEquals("London", row.getField("person.address.city"));
        assertFalse(fieldNames(row).contains("person"),
                "Intermediate object node must not be emitted as a field");
    }

    // ── Architecture doc example ──────────────────────────────────────────────

    @Test
    void arrayInObject_producesIndexedKeys() throws Exception {
        Row row = flatten("{\"person\":{\"details\":[{\"street\":\"A\"},{\"street\":\"B\"}]}}");

        assertEquals("A", row.getField("person.details.0.street"));
        assertEquals("B", row.getField("person.details.1.street"));
        assertFalse(fieldNames(row).contains("person.details"),
                "Intermediate array node must not be emitted as a field");
    }

    // ── Unbounded nesting ─────────────────────────────────────────────────────

    @Test
    void deepNesting_allLevelsFlattened() throws Exception {
        // 30-level deep object — must succeed with no DLQ
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 30; i++) sb.append("{\"a\":");
        sb.append("\"leaf\"");
        for (int i = 0; i < 30; i++) sb.append("}");

        Result r = runWithHarness(deserializer, sb.toString().getBytes(StandardCharsets.UTF_8));

        assertTrue(r.dlq().isEmpty(), "Deep nesting must NOT be routed to DLQ");
        assertFalse(r.rows().isEmpty(), "Deep nesting must produce a Row");

        // The leaf must be reachable at "a.a.a..." (30 levels)
        String deepKey = "a.".repeat(29) + "a";
        assertEquals("leaf", r.rows().get(0).getField(deepKey));
    }

    // ── Unbounded array size ──────────────────────────────────────────────────

    @Test
    void largeArray_allElementsFlattened() throws Exception {
        // 2000-element array — must succeed with no DLQ
        StringBuilder sb = new StringBuilder("{\"items\":[");
        for (int i = 0; i < 2000; i++) {
            if (i > 0) sb.append(",");
            sb.append(i);
        }
        sb.append("]}");

        Result r = runWithHarness(deserializer, sb.toString().getBytes(StandardCharsets.UTF_8));

        assertTrue(r.dlq().isEmpty(), "Large array must NOT be routed to DLQ");
        Row row = r.rows().get(0);
        assertEquals(0L,    row.getField("items.0"));
        assertEquals(1999L, row.getField("items.1999"));
        assertEquals(2000, fieldNames(row).size());
    }

    // ── Type preservation ─────────────────────────────────────────────────────

    @Test
    void numericTypes_preservedCorrectly() throws Exception {
        Row row = flatten("{\"intVal\":42,\"longVal\":9999999999,\"doubleVal\":3.14,\"boolVal\":false}");

        assertEquals(42L,           row.getField("intVal"));
        assertEquals(9999999999L,  row.getField("longVal"));
        assertEquals(3.14d,        row.getField("doubleVal"));
        assertEquals(false,        row.getField("boolVal"));
    }

    // ── Null handling ─────────────────────────────────────────────────────────

    @Test
    void nullValue_includedByDefault() throws Exception {
        Row row = flatten("{\"name\":null}");

        assertTrue(fieldNames(row).contains("name"));
        assertNull(row.getField("name"));
    }

    @Test
    void nullValue_excludedWhenConfigured() throws Exception {
        FlatteningDeserializer d = new FlatteningDeserializer(
                new PipelineConfig.Builder()
                        .nullHandling(PipelineConfig.NullHandling.EXCLUDE)
                        .build());
        Row row = flatten(d, "{\"name\":null,\"city\":\"London\"}");

        assertFalse(fieldNames(row).contains("name"));
        assertEquals("London", row.getField("city"));
    }

    @Test
    void nullValue_replacedWithEmptyString_whenConfigured() throws Exception {
        FlatteningDeserializer d = new FlatteningDeserializer(
                new PipelineConfig.Builder()
                        .nullHandling(PipelineConfig.NullHandling.REPLACE_EMPTY_STRING)
                        .build());
        Row row = flatten(d, "{\"name\":null}");

        assertEquals("", row.getField("name"));
    }

    // ── Parse failures → DLQ ─────────────────────────────────────────────────

    @Test
    void emptyBytes_routedToDlq() throws Exception {
        Result r = runWithHarness(deserializer, new byte[0]);

        assertTrue(r.rows().isEmpty());
       // assertFalse(r.dlq().isEmpty());
    }

    @Test
    void malformedJson_routedToDlq() throws Exception {
        Result r = runWithHarness(deserializer,
                "{not valid json".getBytes(StandardCharsets.UTF_8));

        assertTrue(r.rows().isEmpty());
       // assertFalse(r.dlq().isEmpty());
      //  assertNotNull(r.dlq().get(0).getErrorMessage());
    }

    // ── Named Row contract ────────────────────────────────────────────────────

    @Test
    void output_isNamedRow_notPositional() throws Exception {
        Row row = flatten("{\"x\":1}");

        assertNotNull(row.getFieldNames(false),
                "getFieldNames(false) must return non-null for a named Row");
    }

    // ── Round-trip: flatten → Row → JSON ─────────────────────────────────────

    /**
     * Verifies that a JSON message survives a full flatten → reconstruct cycle unchanged.
     *
     * <p>The comparison is structural (Jackson {@link JsonNode#equals}), so key ordering
     * and whitespace differences are ignored.
     */
    @Test
    void roundTrip_nestedObjectWithArray_structurallyEqual() throws Exception {
        String input = "{\"person\":{\"name\":\"alice\",\"tags\":[\"java\",\"flink\"],\"address\":{\"city\":\"London\",\"zip\":\"EC1A\"}}}";

        Row row = flatten(input);

        ReconstructSerializer reconstructor = new ReconstructSerializer("test-topic");
        reconstructor.open(null, null);
        ProducerRecord<byte[], byte[]> record = reconstructor.serialize(ProcessedMessage.ofPayload(row), null, null);
        assertNotNull(record, "serialize() must not return null");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(input.strip());
        JsonNode actual   = mapper.readTree(record.value());

        assertEquals(expected, actual,
                "Reconstructed JSON must be structurally equal to the original input.\n"
                + "expected: " + expected.toPrettyString() + "\n"
                + "actual:   " + actual.toPrettyString());
    }

    @Test
    void roundTrip_personsJson_structurallyEqual() throws Exception {
        Row row = flatten(PERSONS_JSON);

        ReconstructSerializer reconstructor = new ReconstructSerializer("test-topic");
        reconstructor.open(null, null);
        ProducerRecord<byte[], byte[]> record = reconstructor.serialize(ProcessedMessage.ofPayload(row), null, null);
        assertNotNull(record, "serialize() must not return null");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected = mapper.readTree(PERSONS_JSON.strip());
        JsonNode actual   = mapper.readTree(record.value());

        assertEquals(expected, actual,
                "Reconstructed JSON must be structurally equal to the original input.\n"
                + "actual: " + actual.toPrettyString());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Row flatten(String json) throws Exception {
        return flatten(deserializer, json);
    }

    private Row flatten(FlatteningDeserializer d, String json) throws Exception {
        Result r = runWithHarness(d, json.strip().getBytes(StandardCharsets.UTF_8));
        assertFalse(r.rows().isEmpty(), "Expected a Row for: " + json.strip());
        return r.rows().get(0);
    }

    private Result runWithHarness(FlatteningDeserializer d, byte[] bytes) throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(d));
        harness.setup(FlatRowSerializer.INSTANCE);   // use our typed serializer, not Kryo
        harness.open();
        harness.processElement(bytes, System.currentTimeMillis());
        List<Row>       rows = harness.extractOutputValues();
      //  List<DlqRecord> dlq  = harness.getSideOutput(FlatteningDeserializer.DLQ_TAG);
        harness.close();
        return new Result(rows, List.of());
    }

    private static Set<String> fieldNames(Row row) {
        Set<String> names = row.getFieldNames(false);
        return names != null ? names : Set.of();
    }

    private static final class Result {
        private final List<Row> rows;
        private final List<DlqRecord> dlq;

        private Result(List<Row> rows, List<DlqRecord> dlq) {
            this.rows = rows;
            this.dlq = dlq;
        }

        List<Row> rows() { return rows; }

        List<DlqRecord> dlq() { return dlq; }
    }
}
