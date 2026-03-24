package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.config.PipelineConfig;
import com.pipeline.splitting.SplitFunction;
import com.pipeline.splitting.SplitFunctionFactory;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class JsonRowFlattenerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMPLEX2_SCHEMA_PATH = "schemas/complex2.json";
    private static final String COMPLEX2_INPUT_PATH = "input/complex2-paging.json";

    @Test
    void analyze_extractsRequiredItemFieldsThroughLocalRef2() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema("schemas/input-persons-2-objects-with-metadata.schema.json"));

        assertEquals("persons", inputSchema.getArrayFieldName());
    }

    @Test
    void analyze_extractsRequiredItemFieldsThroughLocalRef3() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema(COMPLEX2_SCHEMA_PATH));

        assertEquals("persons", inputSchema.getArrayFieldName());
        assertTrue(inputSchema.getRequiredItemFields().contains("grades.school"));
        assertTrue(inputSchema.getRequiredItemFields().contains("grades.highschool"));
        assertTrue(inputSchema.getRequiredItemFields().contains("grades.mathGarde"));
        assertTrue(inputSchema.getRequiredItemFields().contains("grades.historyGrade"));
    }
    @Test
    void analyze_extractsRequiredItemFieldsThroughLocalRef4() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema("schemas/complex.json"));

        assertEquals("persons", inputSchema.getArrayFieldName());
        assertTrue(inputSchema.getRequiredItemFields().contains("firstName"));
        assertTrue(inputSchema.getRequiredItemFields().contains("lastName"));
        assertTrue(inputSchema.getRequiredItemFields().contains("address.street"));

        assertFalse(inputSchema.getRequiredItemFields().contains("grades.school"));
        assertFalse(inputSchema.getRequiredItemFields().contains("grades.highschool"));
        assertFalse(inputSchema.getRequiredItemFields().contains("grades.mathGarde"));
        assertFalse(inputSchema.getRequiredItemFields().contains("grades.historyGrade"));

        assertTrue(inputSchema.getConditionalRequiredItemFields().contains("grades.school"));
        assertTrue(inputSchema.getConditionalRequiredItemFields().contains("grades.highschool"));
        assertTrue(inputSchema.getConditionalRequiredItemFields().contains("grades.mathGarde"));
        assertTrue(inputSchema.getConditionalRequiredItemFields().contains("grades.historyGrade"));

        assertEquals("array", inputSchema.getItemFieldTypes().get("grades"));
        assertEquals("integer", inputSchema.getItemFieldTypes().get("grades.mathGarde"));
        assertEquals("integer", inputSchema.getItemFieldTypes().get("grades.historyGrade"));
        assertEquals("integer", inputSchema.getItemFieldTypes().get("grades.school.mathGarde"));
        assertEquals("integer", inputSchema.getItemFieldTypes().get("grades.school.historyGrade"));
        assertEquals("integer", inputSchema.getItemFieldTypes().get("grades.highschool.mathGarde"));
        assertEquals("integer", inputSchema.getItemFieldTypes().get("grades.highschool.historyGrade"));
    }

    @Test
    void analyze_extractsRequiredItemFieldsThroughLocalRef() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema("schemas/input-persons-with-metadata.schema.json"));

        assertEquals("persons", inputSchema.getArrayFieldName());
        assertTrue(inputSchema.getRequiredItemFields().contains("firstName"));
        assertTrue(inputSchema.getRequiredItemFields().contains("lastName"));
        assertTrue(inputSchema.getRequiredItemFields().contains("address"));
        assertTrue(inputSchema.getRequiredItemFields().contains("address.street"));
    }

    @Test
    void validate_missingRequiredFieldInsideArrayItem_reportsIndexedPath() throws Exception {
        JsonNode schema = loadSchema("schemas/input-persons-with-metadata.schema.json");
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(schema);

        ObjectNode invalidJson = (ObjectNode) MAPPER.readTree(loadResource("input/persons-with-metadata.json"));
        ((ObjectNode) invalidJson.get("persons").get(0)).remove("firstName");

        List<String> violations = JsonRowFlattener.validate(invalidJson, inputSchema);

        assertTrue(violations.contains("missing required field 'persons.0.firstName'"),
                "expected indexed array-item violation but got: " + violations);
    }

    @Test
    void validate_complexOneOfBranch_doesNotRequireSiblingBranchFields() throws Exception {
        JsonNode schema = loadSchema("schemas/complex.json");
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(schema);

        String validJsonText = String.join("\n",
                "{",
                "  \"metadata\": {",
                "    \"totalCount\": 1,",
                "    \"page\": 1,",
                "    \"pageSize\": 10,",
                "    \"timestamp\": \"2026-03-14T17:57:00Z\",",
                "    \"apiVersion\": \"1.0\"",
                "  },",
                "  \"persons\": [",
                "    {",
                "      \"firstName\": \"John\",",
                "      \"lastName\": \"Doe\",",
                "      \"age\": 35,",
                "      \"grades\": [",
                "        {",
                "          \"school\": {",
                "            \"mathGarde\": 95,",
                "            \"historyGrade\": 88",
                "          }",
                "        }",
                "      ],",
                "      \"address\": {",
                "        \"street\": \"123 Main St\",",
                "        \"city\": \"Anytown\",",
                "        \"postalCode\": \"12345\",",
                "        \"country\": \"USA\"",
                "      }",
                "    }",
                "  ]",
                "}");
        JsonNode validJson = MAPPER.readTree(validJsonText);

        List<String> violations = JsonRowFlattener.validate(validJson, inputSchema);

        assertTrue(violations.isEmpty(), "oneOf branch selection must not require sibling branch fields: " + violations);
    }

    @Test
    void complex2_personPaging_keepsEachPersonGradesIntact() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema(COMPLEX2_SCHEMA_PATH));
        JsonNode inputJson = MAPPER.readTree(loadResource(COMPLEX2_INPUT_PATH));

        List<String> violations = JsonRowFlattener.validate(inputJson, inputSchema);
        assertTrue(violations.isEmpty(), "complex2 input must be valid before paging: " + violations);

        Row flattened = flattenToRow(inputJson);
        List<Row> pages = splitRows(flattened,
                new PaginationSchema("persons", "metadata.page", "metadata.totalCount", "count"),
                1,
                "persons");

        assertEquals(2, pages.size(), "two persons with pageSize=1 should produce two pages");

        Row page0 = pages.get(0);
        assertEquals(0, page0.getField("metadata.page"));
        assertEquals(2, page0.getField("metadata.totalCount"));
        assertEquals(1, page0.getField("count"));
        assertEquals("2026-03-14T17:57:00Z", page0.getField("metadata.timestamp"));
        assertEquals("John", page0.getField("persons.0.firstName"));
        assertEquals("Doe", page0.getField("persons.0.lastName"));
        assertEquals(35L, page0.getField("persons.0.age"));
        assertEquals(91L, page0.getField("persons.0.grades.0.school.mathGarde"));
        assertEquals(82L, page0.getField("persons.0.grades.0.highschool.historyGrade"));
        assertEquals(96L, page0.getField("persons.0.grades.1.mathGarde"));
        assertEquals(86L, page0.getField("persons.0.grades.1.historyGrade"));
        assertNull(page0.getField("persons.1.firstName"), "person paging must reindex and drop later persons from page 0");

        Row page1 = pages.get(1);
        assertEquals(1, page1.getField("metadata.page"));
        assertEquals(2, page1.getField("metadata.totalCount"));
        assertEquals(1, page1.getField("count"));
        assertEquals("Jane", page1.getField("persons.0.firstName"));
        assertEquals("Smith", page1.getField("persons.0.lastName"));
        assertEquals(31L, page1.getField("persons.0.age"));
        assertEquals(71L, page1.getField("persons.0.grades.0.school.mathGarde"));
        assertEquals(62L, page1.getField("persons.0.grades.0.highschool.historyGrade"));
        assertEquals(76L, page1.getField("persons.0.grades.1.mathGarde"));
        assertEquals(66L, page1.getField("persons.0.grades.1.historyGrade"));
    }

    @Test
    void splitRows_emptySplitPoint_returnsOriginalRowWithoutSplitting() throws Exception {
        JsonNode inputJson = MAPPER.readTree(loadResource(COMPLEX2_INPUT_PATH));
        Row flattened = flattenToRow(inputJson);

        List<Row> rows = splitRows(flattened,
                new PaginationSchema("persons", "metadata.page", "metadata.totalCount", "count"),
                1,
                "");

        assertEquals(1, rows.size(), "blank split point must disable paging");
        Row onlyRow = rows.get(0);
        assertEquals("John", onlyRow.getField("persons.0.firstName"));
        assertEquals("Jane", onlyRow.getField("persons.1.firstName"));
        assertNull(onlyRow.getField("count"), "no-split row must not get pagination metadata");
    }

    @Test
    void splitFunctionAndRuntimeSplitter_emitIdenticalPages() throws Exception {
        JsonNode inputJson = MAPPER.readTree(loadResource(COMPLEX2_INPUT_PATH));
        Row flattened = flattenToRow(inputJson);
        PaginationSchema schema = new PaginationSchema("persons", "metadata.page", "metadata.totalCount", "count");

        List<Row> operatorPages = splitRows(flattened, schema, 1, "persons");

        PipelineConfig inputSplitConfig = new PipelineConfig.Builder()
                .inputFormat(PipelineConfig.InputFormat.JSON)
                .outputFormat(PipelineConfig.OutputFormat.JSON)
                .outputSchemaResource("schemas/output-persons-paged.schema.json")
                .splittingPageSize(1)
                .splitEnabled(true)
                .splitStage(PipelineConfig.SplitStage.INPUT)
                .splitField("persons")
                .build();
        List<ProcessedMessage> runtimePages = SplitFunctionFactory.createRuntime(inputSplitConfig)
                .split(ProcessedMessage.ofPayload(flattened));

        assertEquals(operatorPages.size(), runtimePages.size(), "operator and runtime splitter must emit the same number of pages");
        for (int i = 0; i < operatorPages.size(); i++) {
            Row operatorRow = operatorPages.get(i);
            Row runtimeRow = runtimePages.get(i).getPayload();
            assertEquals(operatorRow.getFieldNames(false), runtimeRow.getFieldNames(false),
                    "page " + i + " must have identical field names");
            for (String fieldName : operatorRow.getFieldNames(false)) {
                assertEquals(operatorRow.getField(fieldName), runtimeRow.getField(fieldName),
                        "page " + i + " field mismatch for " + fieldName);
            }
        }
    }

    private static JsonNode loadSchema(String path) throws Exception {
        return SchemaAnalyzer.loadSchema(path);
    }

    private static Row flattenToRow(JsonNode json) {
        Row row = Row.withNames();
        JsonRowFlattener.flattenInto(json, "", row, PipelineConfig.NullHandling.INCLUDE);
        return row;
    }

    private static List<Row> splitRows(Row row, PaginationSchema schema, int pageSize, String splitPoint) throws Exception {
        PipelineConfig splitConfig = new PipelineConfig.Builder()
                .inputFormat(PipelineConfig.InputFormat.JSON)
                .outputFormat(PipelineConfig.OutputFormat.JSON)
                .splittingPageSize(pageSize)
                .splitField(splitPoint)
                .splitEnabled(splitPoint != null && !splitPoint.isBlank())
                .build();

        if (!splitConfig.isSplitEnabled()) {
            return List.of(row);
        }

        var harness = new OneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(new SplitFunction(schema, splitConfig.getSplittingPageSize(), splitConfig.getSplitField())));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofPayload(row), System.currentTimeMillis());
        List<Row> output = harness.extractOutputValues().stream()
                .map(ProcessedMessage::getPayload)
                .collect(Collectors.toList());
        harness.close();
        return output;
    }

    private static byte[] loadResource(String path) throws Exception {
        try (InputStream is = JsonRowFlattenerTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Classpath resource not found: " + path);
            return is.readAllBytes();
        }
    }
}
