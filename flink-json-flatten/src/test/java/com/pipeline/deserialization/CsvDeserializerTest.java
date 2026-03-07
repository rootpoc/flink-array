package com.pipeline.deserialization;

import com.pipeline.common.typeinfo.FlatRowSerializer;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CsvDeserializer}.
 *
 * <p>Covers happy-path field mapping, column-count mismatch, empty/null input,
 * RFC 4180 parsing, and the {@code parseCsvLine} static helper.
 */
class CsvDeserializerTest {

    private static final String SCHEMA = "schemas/csv-person-flat.schema.json";

    private CsvDeserializer deserializer;

    @BeforeEach
    void setUp() throws Exception {
        deserializer = new CsvDeserializer(SCHEMA);
    }

    // ── Schema loading ────────────────────────────────────────────────────────

    @Test
    void schemaLoaded_columnsMatchRequired() {
        assertEquals(
                List.of("firstName", "lastName", "age", "address.street"),
                deserializer.getColumns());
    }

    // ── Happy-path parsing ────────────────────────────────────────────────────

    @Test
    void validCsvLine_fieldsSetOnRow() throws Exception {
        Row row = parseOne("Alice,Smith,30,5th Ave");

        assertEquals("Alice",   row.getField("firstName"));
        assertEquals("Smith",   row.getField("lastName"));
        assertEquals("30",      row.getField("age"));
        assertEquals("5th Ave", row.getField("address.street"));
    }

    @Test
    void emptyValueInLine_storedAsNull() throws Exception {
        Row row = parseOne("Alice,,30,5th Ave");

        assertEquals("Alice", row.getField("firstName"));
        assertNull(row.getField("lastName"), "empty CSV value must become null");
        assertEquals("30",    row.getField("age"));
    }

    @Test
    void quotedField_decodedCorrectly() throws Exception {
        Row row = parseOne("Alice,\"Smith, Jr.\",30,5th Ave");

        assertEquals("Smith, Jr.", row.getField("lastName"));
    }

    @Test
    void quotedFieldWithEscapedQuote_decodedCorrectly() throws Exception {
        Row row = parseOne("\"Al\"\"ice\",Smith,30,5th Ave");

        assertEquals("Al\"ice", row.getField("firstName"));
    }

    // ── Column count mismatch → no output ────────────────────────────────────

    @Test
    void tooFewColumns_noOutput() throws Exception {
        List<Row> rows = run("Alice,Smith,30"); // only 3, need 4
        assertTrue(rows.isEmpty(), "column mismatch must produce no output");
    }

    @Test
    void tooManyColumns_noOutput() throws Exception {
        List<Row> rows = run("Alice,Smith,30,5th Ave,extra"); // 5 columns
        assertTrue(rows.isEmpty());
    }

    // ── Empty input → no output ───────────────────────────────────────────────

    @Test
    void emptyBytes_noOutput() throws Exception {
        List<Row> rows = runBytes(new byte[0]);
        assertTrue(rows.isEmpty());
    }

    // ── parseCsvLine static helper ────────────────────────────────────────────

    @Test
    void parseCsvLine_simpleFields() {
        assertEquals(List.of("a", "b", "c"), CsvDeserializer.parseCsvLine("a,b,c"));
    }

    @Test
    void parseCsvLine_singleField() {
        assertEquals(List.of("hello"), CsvDeserializer.parseCsvLine("hello"));
    }

    @Test
    void parseCsvLine_emptyString_singleEmptyField() {
        assertEquals(List.of(""), CsvDeserializer.parseCsvLine(""));
    }

    @Test
    void parseCsvLine_trailingComma_addsEmptyField() {
        assertEquals(List.of("a", "b", ""), CsvDeserializer.parseCsvLine("a,b,"));
    }

    @Test
    void parseCsvLine_consecutiveCommas_emptyMiddleField() {
        assertEquals(List.of("a", "", "b"), CsvDeserializer.parseCsvLine("a,,b"));
    }

    @Test
    void parseCsvLine_quotedFieldWithComma() {
        assertEquals(List.of("a,b", "c"), CsvDeserializer.parseCsvLine("\"a,b\",c"));
    }

    @Test
    void parseCsvLine_quotedFieldWithEscapedQuote() {
        assertEquals(List.of("a\"b", "c"), CsvDeserializer.parseCsvLine("\"a\"\"b\",c"));
    }

    @Test
    void parseCsvLine_quotedEmptyField() {
        assertEquals(List.of("a", "", "b"), CsvDeserializer.parseCsvLine("a,\"\",b"));
    }

    @Test
    void parseCsvLine_allQuoted() {
        assertEquals(List.of("first", "last", "42"),
                CsvDeserializer.parseCsvLine("\"first\",\"last\",\"42\""));
    }

    @Test
    void parseCsvLine_fieldWithNewline_preservedInsideQuotes() {
        List<String> fields = CsvDeserializer.parseCsvLine("a,\"line1\nline2\",b");
        assertEquals(3, fields.size());
        assertEquals("line1\nline2", fields.get(1));
    }

    // ── Round-trip with CsvSerializer ─────────────────────────────────────────

    @Test
    void roundTrip_serializeThenDeserialize() throws Exception {
        com.pipeline.serialization.CsvSerializer ser =
                new com.pipeline.serialization.CsvSerializer("t", SCHEMA);

        Row original = Row.withNames();
        original.setField("firstName",      "Alice");
        original.setField("lastName",       "Smith");
        original.setField("age",            "30");
        original.setField("address.street", "5th Ave");

        Row restored = parseOne(ser.toCsvLine(original));

        assertEquals("Alice",   restored.getField("firstName"));
        assertEquals("Smith",   restored.getField("lastName"));
        assertEquals("30",      restored.getField("age"));
        assertEquals("5th Ave", restored.getField("address.street"));
    }

    @Test
    void roundTrip_fieldWithComma() throws Exception {
        com.pipeline.serialization.CsvSerializer ser =
                new com.pipeline.serialization.CsvSerializer("t", SCHEMA);

        Row original = Row.withNames();
        original.setField("firstName",      "Alice");
        original.setField("lastName",       "Smith, Jr.");
        original.setField("age",            "30");
        original.setField("address.street", "5th Ave");

        Row restored = parseOne(ser.toCsvLine(original));

        assertEquals("Smith, Jr.", restored.getField("lastName"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Row parseOne(String csv) throws Exception {
        List<Row> rows = run(csv);
        assertFalse(rows.isEmpty(), "expected at least one output Row for: " + csv);
        return rows.get(0);
    }

    private List<Row> run(String csv) throws Exception {
        return runBytes(csv.getBytes(StandardCharsets.UTF_8));
    }

    private List<Row> runBytes(byte[] bytes) throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(deserializer));
        harness.setup(FlatRowSerializer.INSTANCE);
        harness.open();
        harness.processElement(bytes, System.currentTimeMillis());
        List<Row> rows = harness.extractOutputValues();
        harness.close();
        return rows;
    }
}
