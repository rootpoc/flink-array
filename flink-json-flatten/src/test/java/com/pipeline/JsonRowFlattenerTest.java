package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonRowFlattenerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void analyze_extractsRequiredItemFieldsThroughLocalRef2() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema("schemas/input-persons-2-objects-with-metadata.schema.json"));

        assertEquals("persons", inputSchema.getArrayFieldName());
    }

    @Test
    void analyze_extractsRequiredItemFieldsThroughLocalRef3() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema("schemas/complex2.json"));

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

    private static JsonNode loadSchema(String path) throws Exception {
        return SchemaAnalyzer.loadSchema(path);
    }

    private static byte[] loadResource(String path) throws Exception {
        try (InputStream is = JsonRowFlattenerTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Classpath resource not found: " + path);
            return is.readAllBytes();
        }
    }
}
