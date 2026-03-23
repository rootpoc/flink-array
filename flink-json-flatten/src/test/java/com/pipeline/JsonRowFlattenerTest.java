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

