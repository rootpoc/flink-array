package com.pipeline.serialization;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CsvSerializer}.
 *
 * <p>Covers column ordering, RFC 4180 quoting, null handling, Kafka envelope
 * propagation, and the {@code encodeCsvField} static helper.
 */
class CsvSerializerTest {

    private static final String SCHEMA = "schemas/csv-person-flat.schema.json";

    private CsvSerializer serializer;

    @BeforeEach
    void setUp() throws Exception {
        serializer = new CsvSerializer("csv-output-topic", SCHEMA);
    }

    // ── Schema loading ────────────────────────────────────────────────────────

    @Test
    void schemaLoaded_columnsMatchRequired() {
        assertEquals(
                List.of("firstName", "lastName", "age", "address.street"),
                serializer.getColumns());
    }

    // ── Happy-path CSV line ───────────────────────────────────────────────────

    @Test
    void allFields_producesOrderedCsvLine() {
        Row row = Row.withNames();
        row.setField("firstName",      "Alice");
        row.setField("lastName",       "Smith");
        row.setField("age",            30);
        row.setField("address.street", "5th Ave");

        assertEquals("Alice,Smith,30,5th Ave", serializer.toCsvLine(row));
    }

    @Test
    void extraFieldsInRow_ignoredInOutput() {
        // Row has more fields than schema — only schema columns emitted
        Row row = Row.withNames();
        row.setField("firstName",      "Bob");
        row.setField("lastName",       "Jones");
        row.setField("age",            25);
        row.setField("address.street", "Main St");
        row.setField("email",          "bob@example.com"); // not in schema

        assertEquals("Bob,Jones,25,Main St", serializer.toCsvLine(row));
    }

    // ── Null / missing fields ─────────────────────────────────────────────────

    @Test
    void nullField_serializedAsEmptyString() {
        Row row = Row.withNames();
        row.setField("firstName",      "Carol");
        row.setField("lastName",       null);
        row.setField("age",            40);
        row.setField("address.street", "Oak Ln");

        assertEquals("Carol,,40,Oak Ln", serializer.toCsvLine(row));
    }

    @Test
    void absentField_serializedAsEmptyString() {
        // 'age' and 'address.street' are not set at all
        Row row = Row.withNames();
        row.setField("firstName", "Dan");
        row.setField("lastName",  "Brown");

        assertEquals("Dan,Brown,,", serializer.toCsvLine(row));
    }

    @Test
    void nullRow_allEmptyFields() {
        assertEquals(",,,", serializer.toCsvLine(null));
    }

    // ── RFC 4180 quoting ──────────────────────────────────────────────────────

    @Test
    void fieldWithComma_isQuoted() {
        Row row = Row.withNames();
        row.setField("firstName",      "Alice");
        row.setField("lastName",       "Smith, Jr.");
        row.setField("age",            30);
        row.setField("address.street", "5th Ave");

        assertEquals("Alice,\"Smith, Jr.\",30,5th Ave", serializer.toCsvLine(row));
    }

    @Test
    void fieldWithDoubleQuote_isQuotedAndEscaped() {
        Row row = Row.withNames();
        row.setField("firstName",      "Al\"ice");
        row.setField("lastName",       "Smith");
        row.setField("age",            30);
        row.setField("address.street", "5th Ave");

        assertEquals("\"Al\"\"ice\",Smith,30,5th Ave", serializer.toCsvLine(row));
    }

    @Test
    void fieldWithNewline_isQuoted() {
        Row row = Row.withNames();
        row.setField("firstName",      "Alice");
        row.setField("lastName",       "Smith");
        row.setField("age",            30);
        row.setField("address.street", "line1\nline2");

        String line = serializer.toCsvLine(row);
        assertTrue(line.startsWith("Alice,Smith,30,\"line1"), "address field must be quoted");
        assertTrue(line.contains("\"line1\nline2\""), "newline must be inside quotes");
    }

    // ── encodeCsvField static helper ──────────────────────────────────────────

    @Test
    void encodeCsvField_plainString_noQuotes() {
        assertEquals("hello", CsvSerializer.encodeCsvField("hello"));
    }

    @Test
    void encodeCsvField_null_emptyString() {
        assertEquals("", CsvSerializer.encodeCsvField(null));
    }

    @Test
    void encodeCsvField_containsComma_quoted() {
        assertEquals("\"a,b\"", CsvSerializer.encodeCsvField("a,b"));
    }

    @Test
    void encodeCsvField_containsQuote_escapedAndQuoted() {
        assertEquals("\"say \"\"hi\"\"\"", CsvSerializer.encodeCsvField("say \"hi\""));
    }

    @Test
    void encodeCsvField_integer_asString() {
        assertEquals("42", CsvSerializer.encodeCsvField(42));
    }

    @Test
    void encodeCsvField_boolean_asString() {
        assertEquals("true", CsvSerializer.encodeCsvField(true));
    }

    // ── Kafka ProducerRecord ──────────────────────────────────────────────────

    @Test
    void serialize_correctTopic() {
        Row row = Row.withNames();
        row.setField("firstName", "Eve"); row.setField("lastName", "X");
        row.setField("age", 1); row.setField("address.street", "Y");

        ProducerRecord<byte[], byte[]> record =
                serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);

        assertNotNull(record);
        assertEquals("csv-output-topic", record.topic());
    }

    @Test
    void serialize_valueIsUtf8CsvLine() {
        Row row = Row.withNames();
        row.setField("firstName",      "Alice");
        row.setField("lastName",       "Smith");
        row.setField("age",            30);
        row.setField("address.street", "5th Ave");

        ProducerRecord<byte[], byte[]> record =
                serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);

        assertNotNull(record);
        String csv = new String(record.value(), StandardCharsets.UTF_8);
        assertEquals("Alice,Smith,30,5th Ave", csv);
    }

    @Test
    void serialize_kafkaKeyPropagated() {
        byte[] key = "my-key".getBytes(StandardCharsets.UTF_8);
        Row row = Row.withNames();
        row.setField("firstName", "X"); row.setField("lastName", "Y");
        row.setField("age", 1); row.setField("address.street", "Z");

        ProcessedMessage msg = new ProcessedMessage(key, new byte[0],
                java.util.Map.of(), row);

        ProducerRecord<byte[], byte[]> record =
                serializer.serialize(msg, null, 0L);

        assertNotNull(record);
        assertArrayEquals(key, record.key());
    }
}
