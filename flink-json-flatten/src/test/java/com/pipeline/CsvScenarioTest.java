package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario tests for CSV input backed by an external schema and message fixture.
 *
 * <p>Contract under test:
 * <ul>
 *   <li>All CSV fields are mandatory.</li>
 *   <li>Column order is the exact order of the schema {@code required} array.</li>
 *   <li>The delimiter is a comma ({@code ,}).</li>
 * </ul>
 */
class CsvScenarioTest {

    private static final String SCHEMA_PATH = "schemas/csv-flat-person-required.schema.json";
    private static final String INPUT_PATH  = "input/csv-flat-person-input.csv";

    @Test
    void csvSchema_orderedRequiredColumns_andInputRowParsedCorrectly() throws Exception {
        JsonNode schema = SchemaAnalyzer.loadSchema(SCHEMA_PATH);
        String input = loadText(INPUT_PATH).strip();

        List<String> columns = SchemaAnalyzer.extractCsvColumns(schema);
        assertEquals(List.of(
                "firstName",
                "lastName",
                "age",
                "street",
                "city",
                "postalCode",
                "country"),
                columns,
                "CSV column order must exactly match the schema required array");

        Row row = parseCsvRow(input, columns, schema);

        System.out.println("\n=== CSV parsed row ===");
        columns.forEach(name -> System.out.printf("  %-12s = %s%n", name, row.getField(name)));
        System.out.println("======================\n");

        assertEquals("John",        row.getField("firstName"));
        assertEquals("Doe",         row.getField("lastName"));
        assertEquals(35L,            row.getField("age"));
        assertEquals("123 Main St", row.getField("street"));
        assertEquals("Anytown",     row.getField("city"));
        assertEquals("12345",       row.getField("postalCode"));
        assertEquals("USA",         row.getField("country"));
    }

    @Test
    void csvInput_missingColumn_failsBecauseAllFieldsAreMandatory() throws Exception {
        JsonNode schema = SchemaAnalyzer.loadSchema(SCHEMA_PATH);
        List<String> columns = SchemaAnalyzer.extractCsvColumns(schema);

        String missingCountry = "John,Doe,35,123 Main St,Anytown,12345";

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> parseCsvRow(missingCountry, columns, schema));

        assertTrue(ex.getMessage().contains("Expected 7 CSV fields but got 6"),
                "Error should mention the exact missing-field count, was: " + ex.getMessage());
    }

    private static Row parseCsvRow(String csvLine, List<String> columns, JsonNode schema) {
        String[] values = csvLine.split(",", -1);
        if (values.length != columns.size()) {
            throw new IllegalArgumentException(
                    "Expected " + columns.size() + " CSV fields but got " + values.length);
        }

        Row row = Row.withNames();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String raw = values[i];
            JsonNode fieldSchema = schema.path("properties").path(column);
            row.setField(column, coerce(raw, fieldSchema));
        }
        return row;
    }

    private static Object coerce(String raw, JsonNode fieldSchema) {
        String type = fieldSchema.path("type").asText();
        if ("integer".equals(type)) {
            return Long.parseLong(raw);
        }
        if ("number".equals(type)) {
            return Double.parseDouble(raw);
        }
        if ("boolean".equals(type)) {
            return Boolean.parseBoolean(raw);
        }
        return raw;
    }

    private static String loadText(String path) throws Exception {
        try (InputStream is = CsvScenarioTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Classpath resource not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
