package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@code input-person-of.schema.json}, which uses JSON Schema composition keywords:
 *
 * <ul>
 *   <li>Root {@code required: ["age"]} — age is always required.</li>
 *   <li>Root {@code anyOf} — message must have either {@code firstName} or {@code lastName}
 *       (intersection of the two branches yields no additional required fields, so the validator
 *       enforces only {@code age}).</li>
 *   <li>{@code address.required: ["street"]} — when {@code address} is present, {@code street}
 *       must be present inside it.</li>
 *   <li>{@code address.oneOf} — within address, either {@code city} or {@code state} must be
 *       present (intersection yields no additional required fields).</li>
 * </ul>
 *
 * <p>Effective required paths extracted by {@link InputSchemaInfo#analyze(JsonNode)}:
 * {@code ["age", "address.street"]} — {@code address.street} is only enforced when
 * {@code address} is present (JSON Schema nested-required semantics).
 */
class AnyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static InputSchemaInfo INPUT_SCHEMA;

    @BeforeAll
    static void loadSchema() throws Exception {
        try (InputStream is = AnyTest.class.getClassLoader()
                .getResourceAsStream("schemas/input-person-of.schema.json")) {
            assertNotNull(is, "schemas/input-person-of.schema.json not found on classpath");
            JsonNode schema = MAPPER.readTree(is);
            INPUT_SCHEMA = InputSchemaInfo.analyze(schema);
        }
    }

    // ── Schema inspection ─────────────────────────────────────────────────────

    @Test
    void schema_requiredFieldsAreAgeAndAddressStreet() {
        List<String> required = INPUT_SCHEMA.getRequiredFields();
        assertTrue(required.contains("age"),            "age must be required (root required array)");
        assertTrue(required.contains("address.street"), "address.street must be required (nested required)");
        assertFalse(required.contains("firstName"),     "firstName must NOT be required (anyOf — only in one branch)");
        assertFalse(required.contains("lastName"),      "lastName must NOT be required (anyOf — only in one branch)");
        assertFalse(required.contains("address.city"),  "address.city must NOT be required (oneOf — only in one branch)");
        assertFalse(required.contains("address.state"), "address.state must NOT be required (oneOf — only in one branch)");
        assertNull(INPUT_SCHEMA.getArrayFieldName(), "flat schema — no array field");
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    @Test
    void john_firstNameAndAge_noAddress_validRow() throws Exception {
        byte[] bytes = ("{"
                + "\"firstName\":\"John\","
                + "\"age\":30}").getBytes(StandardCharsets.UTF_8);

        Row row = flatten(bytes);

        assertEquals("John", row.getField("firstName"));
        assertEquals(30L,    row.getField("age"));
        assertNull(row.getField("lastName"),       "lastName not in input — must be absent from row");
        assertNull(row.getField("address.street"), "address not in input — must be absent from row");
    }

    @Test
    void emily_firstNameAgeAndAddress_validRow() throws Exception {
        byte[] bytes = ("{"
                + "\"firstName\":\"Emily\","
                + "\"age\":19,"
                + "\"address\":{"
                + "\"street\":\"456 Maple Avenue\","
                + "\"city\":\"Los Angeles\"}}")
                .getBytes(StandardCharsets.UTF_8);

        Row row = flatten(bytes);

        assertEquals("Emily",            row.getField("firstName"));
        assertEquals(19L,                row.getField("age"));
        assertEquals("456 Maple Avenue", row.getField("address.street"));
        assertEquals("Los Angeles",      row.getField("address.city"));
        assertNull(row.getField("lastName"), "lastName not in input — must be absent from row");
    }

    @Test
    void smith_lastNameAndAge_noAddress_validRow() throws Exception {
        byte[] bytes = ("{"
                + "\"lastName\":\"Smith\","
                + "\"age\":22}").getBytes(StandardCharsets.UTF_8);

        Row row = flatten(bytes);

        assertEquals("Smith", row.getField("lastName"));
        assertEquals(22L,     row.getField("age"));
        assertNull(row.getField("firstName"),      "firstName not in input — must be absent from row");
        assertNull(row.getField("address.street"), "address not in input — must be absent from row");
    }

    // ── DLQ cases ─────────────────────────────────────────────────────────────

    @Test
    void missingAge_routedToDlq() throws Exception {
        byte[] bytes = ("{"
                + "\"firstName\":\"David\"}").getBytes(StandardCharsets.UTF_8);

        DlqRecord dlq = expectDlq(bytes);
        assertTrue(dlq.getErrorMessage().contains("age"),
                "error must mention 'age', was: " + dlq.getErrorMessage());
    }

    @Test
    void addressPresentButStreetMissing_routedToDlq() throws Exception {
        byte[] bytes = ("{"
                + "\"firstName\":\"Bob\","
                + "\"age\":25,"
                + "\"address\":{\"city\":\"Boston\"}}")
                .getBytes(StandardCharsets.UTF_8);

        DlqRecord dlq = expectDlq(bytes);
        assertTrue(dlq.getErrorMessage().contains("address.street"),
                "error must mention 'address.street', was: " + dlq.getErrorMessage());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Runs bytes through {@link ValidateFlattenFunction} and returns the single output Row. */
    private static Row flatten(byte[] bytes) throws Exception {
        var harness = buildHarness();
        harness.processElement(ProcessedMessage.ofValue(bytes), System.currentTimeMillis());
        List<Row> rows = harness.extractOutputValues()
                .stream().map(ProcessedMessage::getPayload).collect(Collectors.toList());
        harness.close();
        assertEquals(1, rows.size(), "expected exactly one Row for a valid message");
        return rows.get(0);
    }

    /** Runs bytes through {@link ValidateFlattenFunction} and asserts exactly one DLQ record. */
    private static DlqRecord expectDlq(byte[] bytes) throws Exception {
        var harness = buildHarness();
        harness.processElement(ProcessedMessage.ofValue(bytes), System.currentTimeMillis());
        assertTrue(harness.extractOutputValues().isEmpty(),
                "no Row must be emitted for an invalid message");

        Queue<?> rawDlq = harness.getSideOutput(ValidateFlattenFunction.DLQ_TAG);
        harness.close();
        assertNotNull(rawDlq, "expected DLQ side output");
        assertEquals(1, rawDlq.size(), "expected exactly one DLQ record");

        List<DlqRecord> dlq = new ArrayList<>();
        rawDlq.forEach(sr -> dlq.add((DlqRecord) ((StreamRecord<?>) sr).getValue()));
        return dlq.get(0);
    }

    private static OneInputStreamOperatorTestHarness<ProcessedMessage, ProcessedMessage> buildHarness()
            throws Exception {
        ValidateFlattenFunction fn = new ValidateFlattenFunction(INPUT_SCHEMA, NullHandling.INCLUDE);
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        return harness;
    }
}
