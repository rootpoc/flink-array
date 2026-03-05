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
 * Unit tests for {@link ReconstructSerializer}.
 *
 * <p>Tests the full round-trip: flat Flink {@link Row} (named-field mode) →
 * JSON bytes → parsed Jackson tree, verifying that the hierarchical structure
 * is correctly reconstructed.
 */
class ReconstructSerializerTest {

    private ReconstructSerializer serializer;
    private ObjectMapper          mapper;

    @BeforeEach
    void setUp() throws Exception {
        serializer = new ReconstructSerializer("output-topic");
        serializer.open(null, null); // initialises ThreadLocal<ObjectMapper>
        mapper = new ObjectMapper();
    }

    // ── Architecture doc example ──────────────────────────────────────────────

    @Test
    void architectureExample_fullRoundTrip() throws Exception {
        Row row = Row.withNames();
        row.setField("person.name",             "ALICE");
        row.setField("person.details.0.street", "A");
        row.setField("person.details.1.street", "B");

        JsonNode root = reconstruct(row);

        assertEquals("ALICE", root.at("/person/name").asText());
        assertEquals("A",     root.at("/person/details/0/street").asText());
        assertEquals("B",     root.at("/person/details/1/street").asText());
        assertTrue(root.at("/person/details").isArray(),
                "/person/details must be a JSON array");
        assertEquals(2, root.at("/person/details").size());
    }

    // ── Top-level flat fields ─────────────────────────────────────────────────

    @Test
    void flatFields_reconstructedAsTopLevelKeys() throws Exception {
        Row row = Row.withNames();
        row.setField("name",   "Bob");
        row.setField("age",    25);
        row.setField("active", true);

        JsonNode root = reconstruct(row);

        assertEquals("Bob", root.get("name").asText());
        assertEquals(25,    root.get("age").intValue());
        assertTrue(root.get("active").booleanValue());
    }

    // ── Null value ────────────────────────────────────────────────────────────

    @Test
    void nullValue_serializedAsJsonNull() throws Exception {
        Row row = Row.withNames();
        row.setField("name", null);

        JsonNode root = reconstruct(row);

        assertNotNull(root.get("name"), "'name' key must be present");
        assertTrue(root.get("name").isNull(), "'name' must be JSON null");
    }

    // ── Array with gap ────────────────────────────────────────────────────────

    @Test
    void array_paddedWithNulls_whenIndicesAreGapped() throws Exception {
        Row row = Row.withNames();
        row.setField("items.2.name", "third"); // indices 0 and 1 absent

        JsonNode root = reconstruct(row);

        assertTrue(root.at("/items").isArray());
        assertEquals(3, root.at("/items").size());
        assertTrue(root.at("/items/0").isNull(), "index 0 must be null-padded");
        assertTrue(root.at("/items/1").isNull(), "index 1 must be null-padded");
        assertEquals("third", root.at("/items/2/name").asText());
    }

    // ── Multiple arrays at the same level ─────────────────────────────────────

    @Test
    void multipleArrays_reconstructedIndependently() throws Exception {
        Row row = Row.withNames();
        row.setField("a.0", "x");
        row.setField("a.1", "y");
        row.setField("b.0", "z");

        JsonNode root = reconstruct(row);

        assertTrue(root.at("/a").isArray());
        assertEquals("x", root.at("/a/0").asText());
        assertEquals("y", root.at("/a/1").asText());
        assertTrue(root.at("/b").isArray());
        assertEquals("z", root.at("/b/0").asText());
    }

    // ── Deeply nested object ──────────────────────────────────────────────────

    @Test
    void deeplyNestedObject_reconstructedCorrectly() throws Exception {
        Row row = Row.withNames();
        row.setField("a.b.c.d", "deep");

        JsonNode root = reconstruct(row);

        assertEquals("deep", root.at("/a/b/c/d").asText());
    }

    // ── Output topic ──────────────────────────────────────────────────────────

    @Test
    void producerRecord_hasCorrectTopic() {
        Row row = Row.withNames();

        ProducerRecord<byte[], byte[]> record = serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);

        assertNotNull(record);
        assertEquals("output-topic", record.topic());
    }

    // ── Utility methods ───────────────────────────────────────────────────────

    @Test
    void splitDotPath_multipleSegments() {
        assertArrayEquals(
                new String[]{"person", "details", "0", "street"},
                ReconstructSerializer.splitDotPath("person.details.0.street"));
    }

    @Test
    void splitDotPath_singleSegment() {
        assertArrayEquals(new String[]{"name"},
                ReconstructSerializer.splitDotPath("name"));
    }

    @Test
    void isNumeric_correctClassification() {
        assertTrue(ReconstructSerializer.isNumeric("0"));
        assertTrue(ReconstructSerializer.isNumeric("123"));
        assertFalse(ReconstructSerializer.isNumeric("abc"));
        assertFalse(ReconstructSerializer.isNumeric(""));
        assertFalse(ReconstructSerializer.isNumeric(null));
        assertFalse(ReconstructSerializer.isNumeric("1a"));
        assertFalse(ReconstructSerializer.isNumeric("-1")); // negative not treated as index
    }

    // ── Positional (non-named) Row — edge case ────────────────────────────────

    @Test
    void positionalRow_emitsEmptyObject() {
        Row positional = new Row(2); // positional, not named
        positional.setField(0, "x");
        positional.setField(1, "y");

        ProducerRecord<byte[], byte[]> record = serializer.serialize(ProcessedMessage.ofPayload(positional), null, 0L);

        assertNotNull(record, "Positional row must not cause a null return");
        // getFieldNames(false) returns null → empty JSON object {}
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JsonNode reconstruct(Row row) throws Exception {
        ProducerRecord<byte[], byte[]> record = serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);
        assertNotNull(record, "serialize() must not return null");
        return mapper.readTree(record.value());
    }
}
