package com.pipeline.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.InputProcessFunctionFactory;
import com.pipeline.OutputProcessFunctionFactory;
import com.pipeline.common.DlqRecord;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round-trip tests for a flat CSV person payload using a single schema for both
 * input parsing and output serialization.
 */
class CsvScenarioTest {

    private static final String SCHEMA_PATH = "schemas/csv-flat-person-required.schema.json";
    private static final String INPUT_PATH  = "input/csv-flat-person-input.csv";

    private static final JsonNode CSV_SCHEMA;
    private static final List<String> CSV_COLUMNS;
    private static final String CSV_INPUT;
    private static final PipelineConfig CSV_CONFIG;

    static {
        try {
            CSV_SCHEMA  = SchemaAnalyzer.loadSchema(SCHEMA_PATH);
            CSV_COLUMNS = SchemaAnalyzer.extractCsvColumns(CSV_SCHEMA);
            CSV_INPUT   = loadText(INPUT_PATH).strip();
            CSV_CONFIG  = new PipelineConfig.Builder()
                    .inputFormat(PipelineConfig.InputFormat.CSV)
                    .outputFormat(PipelineConfig.OutputFormat.CSV)
                    .inputSchemaResource(SCHEMA_PATH)
                    .outputSchemaResource(SCHEMA_PATH)
                    .build();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Test
    void csvRoundTrip_rowIsCorrect_andOutputMatchesInput() throws Exception {
        assertEquals(List.of(
                "firstName",
                "lastName",
                "age",
                "street",
                "city",
                "postalCode",
                "country"),
                CSV_COLUMNS,
                "CSV column order must exactly match the schema required array");

        List<ProcessedMessage> messages = runInput(CSV_CONFIG, CSV_INPUT.getBytes(StandardCharsets.UTF_8));
        assertEquals(1, messages.size(), "input process function must emit exactly one CSV Row");
        Row row = messages.get(0).getPayload();

        System.out.println("\n=== CSV parsed row ===");
        printRow(row, CSV_COLUMNS);

        assertEquals("John",        row.getField("firstName"));
        assertEquals("Doe",         row.getField("lastName"));
        assertEquals(35L,            row.getField("age"));
        assertEquals("123 Main St", row.getField("street"));
        assertEquals("Anytown",     row.getField("city"));
        assertEquals("12345",       row.getField("postalCode"));
        assertEquals("USA",         row.getField("country"));

        Set<String> fieldNames = row.getFieldNames(false);
        assertNotNull(fieldNames, "CSV row must be a named Row");
        assertEquals(7, fieldNames.size(), "All CSV fields must be present in the Row");

        String output = new String(runOutput(CSV_CONFIG, messages.get(0)).getValue(), StandardCharsets.UTF_8);
        System.out.println("CSV output: " + output);

        assertEquals(CSV_INPUT, output,
                "CSV round-trip must preserve exact column order and values when the "
                + "input schema is also used as the output schema.\n"
                + "expected: " + CSV_INPUT + "\n"
                + "actual:   " + output);
    }

    @Test
    void csvInput_missingColumn_failsBecauseAllFieldsAreMandatory() throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(InputProcessFunctionFactory.create(CSV_CONFIG)));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();

        String missingCountry = "John,Doe,35,123 Main St,Anytown,12345";
        harness.processElement(ProcessedMessage.ofValue(missingCountry.getBytes(StandardCharsets.UTF_8)),
                System.currentTimeMillis());

        assertTrue(harness.extractOutputValues().isEmpty(),
                "invalid CSV must not emit a Row");
        assertNotNull(harness.getSideOutput(InputProcessFunctionFactory.DLQ_TAG),
                "invalid CSV must be routed to DLQ side output");
        harness.close();
    }

    @Test
    void csvInput_wrongTypeForAge_routesToDlq() throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(InputProcessFunctionFactory.create(CSV_CONFIG)));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();

        String wrongAge = "John,Doe,abc,123 Main St,Anytown,12345,USA";
        byte[] invalidBytes = wrongAge.getBytes(StandardCharsets.UTF_8);
        harness.processElement(ProcessedMessage.ofValue(invalidBytes), System.currentTimeMillis());

        assertTrue(harness.extractOutputValues().isEmpty(),
                "CSV with a non-numeric age must not emit a Row");

        List<DlqRecord> dlqRecords = harness.getSideOutput(InputProcessFunctionFactory.DLQ_TAG).stream()
                .map(org.apache.flink.streaming.runtime.streamrecord.StreamRecord::getValue)
                .collect(Collectors.toList());
        assertEquals(1, dlqRecords.size(), "invalid CSV must produce exactly one DLQ record");

        DlqRecord dlq = dlqRecords.get(0);
        assertArrayEquals(invalidBytes, dlq.getOriginalBytes(),
                "DLQ record must preserve the invalid CSV payload");
        assertEquals(NumberFormatException.class.getName(), dlq.getErrorClass(),
                "wrong-type age should fail during numeric coercion");
        assertTrue(dlq.getErrorMessage().contains("abc"),
                "DLQ error must mention the invalid age value: " + dlq.getErrorMessage());

        harness.close();
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

    private static String loadText(String path) throws Exception {
        try (InputStream is = CsvScenarioTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Classpath resource not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void printRow(Row row, List<String> columns) {
        columns.forEach(name -> System.out.printf("  %-12s = %s%n", name, row.getField(name)));
        System.out.println("======================\n");
    }
}
