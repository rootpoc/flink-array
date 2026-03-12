package com.pipeline.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.DlqRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link MessageValidatorFunction}.
 *
 * <h2>Two scenarios under test</h2>
 * <ol>
 *   <li>Incoming message is missing a <b>required top-level field</b> → routed to DLQ.</li>
 *   <li>Incoming message has the top-level array but one or more <b>array items are
 *       missing a required field</b> → routed to DLQ.</li>
 * </ol>
 *
 * <p>Both use the Flink {@link OneInputStreamOperatorTestHarness} so the full
 * side-output / DLQ routing behaviour is exercised, not just the pure validation logic.
 */
class MessageValidatorFunctionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Test configuration ────────────────────────────────────────────────────

    /**
     * Validator aligned with {@code input-person.schema.json}:
     * <ul>
     *   <li>Top-level: {@code "persons"} must be present.</li>
     *   <li>Per person: {@code firstName}, {@code lastName}, {@code age} required.</li>
     * </ul>
     */
    private static MessageValidatorFunction personValidator() {
        return new MessageValidatorFunction(
                "persons",
                List.of("persons"),
                List.of("firstName", "lastName", "age"));
    }

    // ── Harness helpers ───────────────────────────────────────────────────────

    private Result run(MessageValidatorFunction fn, byte[] input) throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
                .createSerializer(new ExecutionConfig()));
        harness.open();
        harness.processElement(input, System.currentTimeMillis());
        List<byte[]> out = harness.extractOutputValues();

        Queue<?> rawDlq = harness.getSideOutput(MessageValidatorFunction.DLQ_TAG);
        List<DlqRecord> dlq = new ArrayList<>();
        if (rawDlq != null) {
            rawDlq.forEach(sr -> dlq.add(
                    (DlqRecord) ((StreamRecord<?>) sr).getValue()));
        }
        harness.close();
        return new Result(out, dlq);
    }

    private static byte[] json(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class Result {
        private final List<byte[]> out;
        private final List<DlqRecord> dlq;

        private Result(List<byte[]> out, List<DlqRecord> dlq) {
            this.out = out;
            this.dlq = dlq;
        }

        List<byte[]> out() { return out; }

        List<DlqRecord> dlq() { return dlq; }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Scenario 1 — missing top-level required field
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * The incoming JSON has no {@code "persons"} field at the root level.
     * The validator must reject the message and route it to the DLQ.
     */
    @Test
    void missingTopLevelRequiredField_routedToDlq() throws Exception {
        // "persons" is absent; array is stored under "data" instead
        byte[] input = json("{\"data\":[{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30}]}");

        Result r = run(personValidator(), input);

        assertTrue(r.out().isEmpty(),   "no main output when top-level field is missing");
        assertEquals(1, r.dlq().size(), "exactly one DLQ record expected");
        assertTrue(r.dlq().get(0).getErrorMessage().contains("persons"),
                "error message must identify the missing top-level field");
    }

    @Test
    void missingTopLevelRequiredField_errorMessageContainsFieldName() throws Exception {
        MessageValidatorFunction fn = new MessageValidatorFunction(
                "items",
                List.of("items", "requestId"),   // two top-level required fields
                List.of());

        // "requestId" is missing
        byte[] input = json("{\"items\":[]}");
        Result r = run(fn, input);

        assertTrue(r.out().isEmpty());
        assertTrue(r.dlq().get(0).getErrorMessage().contains("requestId"),
                "DLQ error must name the missing field 'requestId'");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Scenario 2 — required field missing inside an array item
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * The {@code "persons"} array is present but the first (and only) person
     * is missing {@code "lastName"}.  The whole message must be routed to the DLQ.
     */
    @Test
    void arrayItemMissingRequiredField_routedToDlq() throws Exception {
        byte[] input = json("{\"persons\":[{\"firstName\":\"John\",\"age\":30}]}");

        Result r = run(personValidator(), input);

        assertTrue(r.out().isEmpty(),   "no main output when item field is missing");
        assertEquals(1, r.dlq().size(), "exactly one DLQ record expected");
        String msg = r.dlq().get(0).getErrorMessage();
        assertTrue(msg.contains("lastName"),    "error must name the missing field");
        assertTrue(msg.contains("persons[0]"),  "error must identify the array index");
    }

    @Test
    void arrayItemMissingMultipleFields_allViolationsReportedInDlq() throws Exception {
        // Person is missing both "lastName" and "age"
        byte[] input = json("{\"persons\":[{\"firstName\":\"John\"}]}");
        Result r = run(personValidator(), input);

        assertTrue(r.out().isEmpty());
        String msg = r.dlq().get(0).getErrorMessage();
        assertTrue(msg.contains("lastName"), "'lastName' violation must be reported");
        assertTrue(msg.contains("age"),      "'age' violation must be reported");
    }

    @Test
    void lastArrayItemMissingField_correctIndexReportedInDlq() throws Exception {
        // First two persons are valid; the third is missing "age"
        byte[] input = json("{\"persons\":["
                + "{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30},"
                + "{\"firstName\":\"Jane\",\"lastName\":\"Smith\",\"age\":28},"
                + "{\"firstName\":\"Bob\",\"lastName\":\"Jones\"}]}");

        Result r = run(personValidator(), input);

        assertTrue(r.out().isEmpty());
        String msg = r.dlq().get(0).getErrorMessage();
        assertTrue(msg.contains("age"),        "missing field must be named");
        assertTrue(msg.contains("persons[2]"), "index 2 (0-based) must be identified");
    }

    @Test
    void multipleItemsMissingFields_allViolationsCollectedInSingleDlqRecord() throws Exception {
        // Every person is missing "age"
        byte[] input = json("{\"persons\":["
                + "{\"firstName\":\"Alice\",\"lastName\":\"A\"},"
                + "{\"firstName\":\"Bob\",\"lastName\":\"B\"},"
                + "{\"firstName\":\"Carol\",\"lastName\":\"C\"}]}");

        Result r = run(personValidator(), input);

        assertTrue(r.out().isEmpty());
        assertEquals(1, r.dlq().size(), "one DLQ record for the whole message, not per item");
        String msg = r.dlq().get(0).getErrorMessage();
        // All three indexes reported
        assertTrue(msg.contains("persons[0]"), "index 0 violation missing");
        assertTrue(msg.contains("persons[1]"), "index 1 violation missing");
        assertTrue(msg.contains("persons[2]"), "index 2 violation missing");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Valid messages — pass through unchanged
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void validMessage_allFieldsPresent_passesThroughUnchanged() throws Exception {
        byte[] input = json("{\"persons\":[{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30}]}");

        Result r = run(personValidator(), input);

        assertEquals(1, r.out().size(),     "exactly one main output expected");
        assertTrue(r.dlq().isEmpty(),       "no DLQ record for valid message");
        assertArrayEquals(input, r.out().get(0),
                "output bytes must be identical to input bytes (pass-through)");
    }

    @Test
    void validMessage_multiplePersonsAllPresent_passesThrough() throws Exception {
        byte[] input = json("{\"persons\":["
                + "{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30},"
                + "{\"firstName\":\"Jane\",\"lastName\":\"Smith\",\"age\":28}]}");

        Result r = run(personValidator(), input);

        assertEquals(1, r.out().size());
        assertTrue(r.dlq().isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Edge cases
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void emptyBytesInput_routedToDlq() throws Exception {
        Result r = run(personValidator(), new byte[0]);

        assertTrue(r.out().isEmpty());
        assertFalse(r.dlq().isEmpty());
    }

    @Test
    void noItemRequirements_topLevelOnlyValidator_passesIncompleteItems() throws Exception {
        // Validator that only checks top-level; items can have any structure
        MessageValidatorFunction fn = new MessageValidatorFunction(
                "persons", List.of("persons"), List.of());

        byte[] input = json("{\"persons\":[{\"whatever\":true}]}");
        Result r = run(fn, input);

        assertEquals(1, r.out().size(), "no item validation → passes through");
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void emptyArray_noItemsToValidate_passesThrough() throws Exception {
        byte[] input = json("{\"persons\":[]}");
        Result r = run(personValidator(), input);

        assertEquals(1, r.out().size(), "empty array has no items to fail validation");
        assertTrue(r.dlq().isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Unit test of the pure validate() logic (no Flink harness)
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void validate_missingTopLevelField_returnsViolation() throws Exception {
        MessageValidatorFunction fn = new MessageValidatorFunction(
                "persons", List.of("persons", "tenantId"), List.of());

        JsonNode root = MAPPER.readTree("{\"persons\":[]}");
        List<String> violations = fn.validate(root);

        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("tenantId"));
    }

    @Test
    void validate_missingItemField_returnsViolationWithIndex() throws Exception {
        MessageValidatorFunction fn = new MessageValidatorFunction(
                "persons", List.of(), List.of("name"));

        JsonNode root = MAPPER.readTree("{\"persons\":[{\"age\":30}]}");
        List<String> violations = fn.validate(root);

        assertEquals(1, violations.size());
        assertTrue(violations.get(0).contains("name"));
        assertTrue(violations.get(0).contains("[0]"));
    }

    @Test
    void validate_allPresent_returnsEmptyList() throws Exception {
        MessageValidatorFunction fn = new MessageValidatorFunction(
                "persons", List.of("persons"), List.of("name"));

        JsonNode root = MAPPER.readTree("{\"persons\":[{\"name\":\"Alice\"}]}");
        List<String> violations = fn.validate(root);

        assertTrue(violations.isEmpty());
    }
}
