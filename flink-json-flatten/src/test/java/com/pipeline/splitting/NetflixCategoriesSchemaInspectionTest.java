package com.pipeline.splitting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Inspects the structure of {@code input-netflix-categories.schema.json}
 * as collected by {@link SchemaAnalyzer#collectProperties}.
 *
 * <p>This schema is an <em>input</em> schema — it has no pagination fields,
 * so {@code SchemaAnalyzer.analyze()} would throw. Instead we verify the
 * top-level properties that {@code collectProperties} discovers.
 */
class NetflixCategoriesSchemaInspectionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode loadSchema() throws Exception {
        try (InputStream is = NetflixCategoriesSchemaInspectionTest.class
                .getClassLoader()
                .getResourceAsStream("schemas/input-netflix-categories.schema.json")) {
            assertNotNull(is, "Schema resource not found");
            return MAPPER.readTree(is);
        }
    }

    @Test
    void collectProperties_netflixCategoriesSchema_findsTopLevelProperties() throws Exception {
        JsonNode schema = loadSchema();

        Map<String, JsonNode> props = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, props);

        // Top-level properties: "application" and "search_engines"
        assertEquals(2, props.size(), "Expected exactly 2 top-level properties");
        assertTrue(props.containsKey("application"),    "Expected 'application' property");
        assertTrue(props.containsKey("search_engines"), "Expected 'search_engines' property");
    }

    @Test
    void collectProperties_applicationProperty_isObjectType() throws Exception {
        JsonNode schema = loadSchema();

        Map<String, JsonNode> props = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, props);

        JsonNode application = props.get("application");
        assertEquals("object", application.path("type").asText(),
                "'application' should be of type object");
    }

    @Test
    void collectProperties_searchEnginesProperty_isArrayType() throws Exception {
        JsonNode schema = loadSchema();

        Map<String, JsonNode> props = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, props);

        JsonNode searchEngines = props.get("search_engines");
        assertEquals("array", searchEngines.path("type").asText(),
                "'search_engines' should be of type array");
    }

    @Test
    void collectProperties_searchEnginesItems_hasExpectedFields() throws Exception {
        JsonNode schema = loadSchema();

        Map<String, JsonNode> props = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, props);

        // Inspect the items sub-schema of search_engines
        JsonNode items = props.get("search_engines").path("items");
        assertEquals("object", items.path("type").asText(),
                "search_engines items should be of type object");

        JsonNode itemProps = items.path("properties");
        assertTrue(itemProps.has("category"),  "items should have 'category'");
        assertTrue(itemProps.has("name"),      "items should have 'name'");
        assertTrue(itemProps.has("list"),      "items should have 'list'");
        assertTrue(itemProps.has("pinned"),    "items should have 'pinned'");
        assertTrue(itemProps.has("last_used"), "items should have 'last_used'");
        assertTrue(itemProps.has("imdb"),      "items should have 'imdb'");
    }

    @Test
    void collectProperties_applicationRequiredFields_arePresent() throws Exception {
        JsonNode schema = loadSchema();

        Map<String, JsonNode> props = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, props);

        JsonNode required = props.get("application").path("required");
        assertTrue(required.isArray(), "'application' should have a required array");

        // Collect required field names
        java.util.Set<String> requiredFields = new java.util.HashSet<>();
        required.forEach(n -> requiredFields.add(n.asText()));

        assertTrue(requiredFields.contains("name"),    "application requires 'name'");
        assertTrue(requiredFields.contains("app_id"),  "application requires 'app_id'");
        assertTrue(requiredFields.contains("version"), "application requires 'version'");
    }

    @Test
    void analyze_netflixCategoriesSchema_throwsBecauseNoPaginationFields() throws Exception {
        JsonNode schema = loadSchema();

        // This is an input schema — no pagination integer fields exist at top level,
        // so analyze() cannot identify total/index/count roles.
        assertThrows(IllegalArgumentException.class,
                () -> SchemaAnalyzer.analyze(schema),
                "analyze() should throw for a non-paginated input schema");
    }
}
