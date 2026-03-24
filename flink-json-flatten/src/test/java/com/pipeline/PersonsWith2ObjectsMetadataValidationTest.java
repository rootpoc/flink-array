package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class PersonsWith2ObjectsMetadataValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SCHEMA_PATH = "schemas/input-persons-2-objects-with-metadata.schema.json";
    private static final String INPUT_PATH = "input/persons-with-2-objects-metadata.json";

    @Test
    void correctInput() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema(SCHEMA_PATH));
        JsonNode inputJson = MAPPER.readTree(loadResource(INPUT_PATH));

        List<String> violations = JsonRowFlattener.validate(inputJson, inputSchema);

        assertTrue(violations.isEmpty(), "expected valid input but got violations: " + violations);
    }

    @Test
    void inputMissingMathGardeAndStreet() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema(SCHEMA_PATH));
        ObjectNode inputJson = (ObjectNode) MAPPER.readTree(loadResource(INPUT_PATH));
        ObjectNode firstPerson = (ObjectNode) inputJson.get("persons").get(0);

        ((ObjectNode) firstPerson.get("grades")).remove("mathGarde");
        ((ObjectNode) firstPerson.get("address")).remove("street");

        List<String> violations = JsonRowFlattener.validate(inputJson, inputSchema);
        List<String> uniqueViolations = violations.stream().distinct().collect(Collectors.toList());

        assertTrue(uniqueViolations.contains("missing required field 'persons.0.grades.mathGarde'"),
                "expected missing mathGarde violation but got: " + violations);
        assertTrue(uniqueViolations.contains("missing required field 'persons.0.address.street'"),
                "expected missing street violation but got: " + violations);
        assertEquals(2, uniqueViolations.size(), "expected exactly two logical missing-field violations: " + violations);
    }

    @Test
    void incorrectTypeForAgeAndHistoryGrade() throws Exception {
        InputSchemaInfo inputSchema = InputSchemaInfo.analyze(loadSchema(SCHEMA_PATH));
        ObjectNode inputJson = (ObjectNode) MAPPER.readTree(loadResource(INPUT_PATH));
        ObjectNode firstPerson = (ObjectNode) inputJson.get("persons").get(0);

        firstPerson.put("age", "thirty-five");
        ((ObjectNode) firstPerson.get("grades")).put("historyGrade", "A+");

        List<String> violations = JsonRowFlattener.validate(inputJson, inputSchema);
        List<String> uniqueViolations = violations.stream().distinct().collect(Collectors.toList());

        assertTrue(uniqueViolations.contains("incorrect type for field 'persons.0.age': expected integer but was string"),
                "expected age type violation but got: " + violations);
        assertTrue(uniqueViolations.contains("incorrect type for field 'persons.0.grades.historyGrade': expected integer but was string"),
                "expected historyGrade type violation but got: " + violations);
        assertEquals(2, uniqueViolations.size(), "expected exactly two logical type violations: " + violations);
    }

    private static JsonNode loadSchema(String path) throws Exception {
        return SchemaAnalyzer.loadSchema(path);
    }

    private static byte[] loadResource(String path) throws Exception {
        try (InputStream is = PersonsWith2ObjectsMetadataValidationTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Classpath resource not found: " + path);
            return is.readAllBytes();
        }
    }
}
