package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.serialization.ReconstructSerializer;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.common.PaginationSchema;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip test using {@code input-person.schema.json} as both the input and output schema.
 *
 * <p>Because the same schema governs both sides and no transformation is applied,
 * every field in the output message must be identical to the corresponding field
 * in the input message — nothing added, nothing removed, nothing changed.
 *
 * <p>Pipeline under test:
 * <pre>
 *   byte[] (person JSON)
 *     → ValidateSplitFlattenFunction  (JSON → Row, dot-notation keys)
 *     → ReconstructSerializer         (Row → JSON bytes)
 * </pre>
 *
 * <p>The person schema has no array-typed property, so {@link InputSchemaAnalyzer} sets
 * {@code arrayFieldName = null} and {@link ValidateSplitFlattenFunction} flattens the whole
 * message into a single {@link Row} — the same code path used for all schemas.
 */
class EyalTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Input message — valid person matching input-person.schema.json ─────────

    private static final String PERSON_JSON = """
            {
              "firstName": "John",
              "lastName":  "Doe",
              "age":       30,
              "address": {
                "street":     "123 Main St",
                "city":       "Springfield",
                "state":      "IL",
                "postalCode": "62701",
                "country":    "US"
              }
            }""";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static JsonNode loadSchema(String classpathPath) throws Exception {
        try (InputStream is = EyalTest.class.getClassLoader()
                .getResourceAsStream(classpathPath)) {
            assertNotNull(is, "Schema not found on classpath: " + classpathPath);
            return MAPPER.readTree(is);
        }
    }

    private static byte[] loadBytes(String classpathPath) throws Exception {
        try (InputStream is = EyalTest.class.getClassLoader()
                .getResourceAsStream(classpathPath)) {
            assertNotNull(is, "Resource not found on classpath: " + classpathPath);
            return is.readAllBytes();
        }
    }

    /** Flatten {@code bytes} to a single Row via {@link ValidateSplitFlattenFunction}. */
    private static Row flatten(byte[] bytes, InputSchemaInfo inputSchema) throws Exception {
        // outputSchema is null — not accessed when arrayFieldName is null (flat schema)
        ValidateSplitFlattenFunction fn = new ValidateSplitFlattenFunction(
                inputSchema, null, 100, NullHandling.INCLUDE);
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(bytes), System.currentTimeMillis());
        List<Row> rows = harness.<ProcessedMessage>extractOutputValues()
                .stream().map(ProcessedMessage::getPayload).toList();
        harness.close();
        assertEquals(1, rows.size(), "expected exactly one Row from the function");
        return rows.get(0);
    }

    /** Reconstruct a Row back to JSON bytes via {@link ReconstructSerializer}. */
    private static JsonNode reconstruct(Row row) throws Exception {
        ReconstructSerializer serializer = new ReconstructSerializer("output-topic");
        serializer.open(null, null);
        ProducerRecord<byte[], byte[]> record = serializer.serialize(ProcessedMessage.ofPayload(row), null, 0L);
        assertNotNull(record, "serializer must not return null");
        return MAPPER.readTree(record.value());
    }

    // ── Test ──────────────────────────────────────────────────────────────────

    @Test
    void personSchema_inputEqualsOutput_noFieldChanges() throws Exception {

        // ── 1. Load schema (used as both input and output schema) ──────────────
        JsonNode schema = loadSchema("schemas/input-person-of.schema.json");
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(schema);

        assertNull(inputSchema.getArrayFieldName(),
                "person schema must be detected as a flat schema (no array field)");

        // ── 2. Parse original input ────────────────────────────────────────────
        byte[]   inputBytes = PERSON_JSON.strip().getBytes(StandardCharsets.UTF_8);
        JsonNode inputNode  = MAPPER.readTree(inputBytes);

        // ── 3. Run through ValidateSplitFlattenFunction → ReconstructSerializer ─
        Row      row        = flatten(inputBytes, inputSchema);
        JsonNode outputNode = reconstruct(row);

        // ── 4. Verify required fields from the schema are unchanged ────────────
        JsonNode required = schema.path("required");
        assertTrue(required.isArray(), "schema must declare a 'required' array");

        required.forEach(fieldNameNode -> {
            String   field  = fieldNameNode.asText();
            JsonNode inVal  = inputNode.path(field);
            JsonNode outVal = outputNode.path(field);
            assertFalse(inVal.isMissingNode(),  "input must have required field: "  + field);
            assertFalse(outVal.isMissingNode(), "output must have required field: " + field);
            assertEquals(inVal, outVal,
                    "required field '" + field + "' must be unchanged after round-trip");
        });

        // ── 5. Verify the nested address object is fully preserved ─────────────
        JsonNode inAddr  = inputNode.path("address");
        JsonNode outAddr = outputNode.path("address");

        assertFalse(inAddr.isMissingNode(),  "input must contain 'address'");
        assertFalse(outAddr.isMissingNode(), "output must contain 'address'");
        assertTrue(outAddr.isObject(),       "output 'address' must be a JSON object");

        Iterator<Map.Entry<String, JsonNode>> addrFields = inAddr.fields();
        while (addrFields.hasNext()) {
            Map.Entry<String, JsonNode> entry = addrFields.next();
            String   key    = entry.getKey();
            JsonNode inVal  = entry.getValue();
            JsonNode outVal = outAddr.path(key);
            assertFalse(outVal.isMissingNode(),
                    "address field '" + key + "' must be present in output");
            assertEquals(inVal, outVal,
                    "address field '" + key + "' must be unchanged after round-trip");
        }

        // ── 6. Verify no extra fields were added to the output ─────────────────
        assertEquals(inputNode.size(), outputNode.size(),
                "output must have the same number of top-level fields as input");
    }

    @Test
    void missingRequiredField_firstName_routedToDlq() throws Exception {

        // ── 1. Load schema ─────────────────────────────────────────────────────
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(
                loadSchema("schemas/input-person.schema.json"));

        // ── 2. Message with firstName omitted ──────────────────────────────────
        byte[] inputBytes = """
                {
                  "lastName": "Doe",
                  "age":      30
                }""".strip().getBytes(StandardCharsets.UTF_8);

        // ── 3. Run through ValidateSplitFlattenFunction ─────────────────────────
        ValidateSplitFlattenFunction fn = new ValidateSplitFlattenFunction(
                inputSchema, null, 100, NullHandling.INCLUDE);
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(inputBytes), System.currentTimeMillis());

        // ── 4. Main output must be empty — no Row emitted for an invalid message ─
        assertTrue(harness.extractOutputValues().isEmpty(),
                "no Row must be emitted for a message that fails validation");

        // ── 5. DLQ must contain exactly one record ─────────────────────────────
        var dlqRecords = harness.getSideOutput(ValidateSplitFlattenFunction.DLQ_TAG);
        assertEquals(1, dlqRecords.size(), "exactly one DLQ record expected");

        DlqRecord dlq = dlqRecords.iterator().next().getValue();

        // ── 6. DLQ record carries the original bytes unchanged ─────────────────
        assertArrayEquals(inputBytes, dlq.getOriginalBytes(),
                "DLQ record must contain the original raw bytes");

        // ── 7. Error message identifies the missing field ──────────────────────
        assertTrue(dlq.getErrorMessage().contains("firstName"),
                "error message must mention 'firstName', was: " + dlq.getErrorMessage());

        harness.close();
    }

    @Test
    void personArraySchema_threeValidOneInvalid_threeRowsOneDlq() throws Exception {

        // ── 1. Load schemas ─────────────────────────────────────────────────────
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(
                loadSchema("schemas/input-person-array,schema.json"));
        PaginationSchema outputSchema = SchemaAnalyzer.analyze(
                loadSchema("schemas/output-persons-paginated.schema.json"));

        assertEquals("persons", inputSchema.getArrayFieldName(),
                "array field must be 'persons'");

        // ── 2. Load input — 4 persons: John, Jane, Michael (valid), Davis (missing firstName)
        byte[] inputBytes = loadBytes("input/person-array.json");

        // ── 3. Run through ValidateSplitFlattenFunction with pageSize=1 ──────────
        ValidateSplitFlattenFunction fn = new ValidateSplitFlattenFunction(
                inputSchema, outputSchema, 1, NullHandling.INCLUDE);
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(inputBytes), System.currentTimeMillis());

        // ── 4. Verify 3 rows were emitted for the 3 valid persons ─────────────
        List<Row> rows = harness.<ProcessedMessage>extractOutputValues()
                .stream().map(ProcessedMessage::getPayload).toList();
        assertEquals(3, rows.size(), "expected 3 rows for 3 valid persons");

        // Row 0 — John Doe (page index 0, total 4 pages, 1 item)
        Row row0 = rows.get(0);
        assertEquals("John", row0.getField("persons.0.firstName"));
        assertEquals("Doe",  row0.getField("persons.0.lastName"));
        assertEquals(30L,     row0.getField("persons.0.age"));
        assertEquals(0,      row0.getField("index"));
        assertEquals(4,      row0.getField("total"));
        assertEquals(1,      row0.getField("count"));

        // Row 1 — Jane Smith (page index 1)
        Row row1 = rows.get(1);
        assertEquals("Jane",  row1.getField("persons.0.firstName"));
        assertEquals("Smith", row1.getField("persons.0.lastName"));
        assertEquals(1,       row1.getField("index"));

        // Row 2 — Michael Johnson (page index 2)
        Row row2 = rows.get(2);
        assertEquals("Michael",  row2.getField("persons.0.firstName"));
        assertEquals("Johnson",  row2.getField("persons.0.lastName"));
        assertEquals(2,          row2.getField("index"));

        // ── 5. Verify 1 DLQ record — Davis (persons[3]) missing firstName ──────
        var dlqRecords = harness.getSideOutput(ValidateSplitFlattenFunction.DLQ_TAG);
        assertEquals(1, dlqRecords.size(), "expected exactly 1 DLQ record for the invalid person");

        DlqRecord dlq = dlqRecords.iterator().next().getValue();
        assertArrayEquals(inputBytes, dlq.getOriginalBytes(),
                "DLQ record must carry the original message bytes");
        assertTrue(dlq.getErrorMessage().contains("firstName"),
                "DLQ error must mention 'firstName', was: " + dlq.getErrorMessage());

        harness.close();
    }

    @Test
    void missingNestedRequiredField_street_routedToDlq() throws Exception {

        // ── 1. Load schema — same file for input and output ────────────────────
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(
                loadSchema("schemas/input-person.schema.json"));

        // address.street is required inside address; verify the analyzer picks it up
        assertTrue(inputSchema.getRequiredFields().contains("address.street"),
                "address.street must appear in requiredFields: " + inputSchema.getRequiredFields());

        // ── 2. Message: firstName/lastName/age present, address present but street missing ─
        byte[] inputBytes = """
                {
                  "firstName": "John",
                  "lastName":  "Doe",
                  "age":       30,
                  "address": {
                    "city":    "Springfield",
                    "state":   "IL"
                  }
                }""".strip().getBytes(StandardCharsets.UTF_8);

        // ── 3. Run through ValidateSplitFlattenFunction ─────────────────────────
        ValidateSplitFlattenFunction fn = new ValidateSplitFlattenFunction(
                inputSchema, null, 100, NullHandling.INCLUDE);
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(inputBytes), System.currentTimeMillis());

        // ── 4. Main output must be empty — validation must have failed ─────────
        assertTrue(harness.extractOutputValues().isEmpty(),
                "no Row must be emitted when a nested required field is missing");

        // ── 5. DLQ must contain exactly one record ─────────────────────────────
        var dlqRecords = harness.getSideOutput(ValidateSplitFlattenFunction.DLQ_TAG);
        assertEquals(1, dlqRecords.size(), "exactly one DLQ record expected");

        DlqRecord dlq = dlqRecords.iterator().next().getValue();
        assertArrayEquals(inputBytes, dlq.getOriginalBytes(),
                "DLQ record must contain the original raw bytes");
        assertTrue(dlq.getErrorMessage().contains("address.street"),
                "error message must mention 'address.street', was: " + dlq.getErrorMessage());

        harness.close();
    }
}
