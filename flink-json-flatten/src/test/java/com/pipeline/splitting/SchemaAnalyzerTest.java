package com.pipeline.splitting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pipeline.common.PaginationSchema;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SchemaAnalyzer}.
 *
 * <p>All tests are pure Jackson — no Flink dependency required.
 */
class SchemaAnalyzerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static JsonNode loadClasspathSchema(String path) throws Exception {
        try (InputStream is = SchemaAnalyzerTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Schema resource not found: " + path);
            return MAPPER.readTree(is);
        }
    }

    /** Builds the persons-paginated property set as an ObjectNode (reused across tests). */
    private static ObjectNode personsProps(ObjectNode parent) {
        parent.putObject("persons")
                .put("type", "array")
                .put("description", "Persons in this page");
        parent.putObject("index")
                .put("type", "integer")
                .put("minimum", 0)
                .put("description", "0-based page index");
        parent.putObject("total")
                .put("type", "integer")
                .put("minimum", 1)
                .put("description", "Total pages");
        parent.putObject("count")
                .put("type", "integer")
                .put("description", "Items in this page");
        return parent;
    }

    private static void assertPersonsSchema(PaginationSchema ps) {
        assertEquals("persons", ps.getArrayFieldName(), "array field");
        assertEquals("index",   ps.getIndexFieldName(), "index field");
        assertEquals("total",   ps.getTotalFieldName(), "total field");
        assertEquals("count",   ps.getCountFieldName(), "count field");
    }

    // ── Real resource file ─────────────────────────────────────────────────────

    @Test
    void analyze_personsPaginatedSchema() throws Exception {
        JsonNode schema = loadClasspathSchema("schemas/output-persons-paginated.schema.json");
        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    // ── No required array ─────────────────────────────────────────────────────

    @Test
    void analyze_schemaWithoutRequired() {
        // All four pagination fields present in properties; no required array at all
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        personsProps(schema.putObject("properties"));

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    // ── Composition keywords ──────────────────────────────────────────────────

    @Test
    void analyze_allOf_propertiesSplitAcrossSubSchemas() {
        // Properties distributed over two allOf branches
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode allOf = schema.putArray("allOf");

        personsProps(allOf.addObject().putObject("properties"));  // all four in one branch

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_allOf_eachBranchHasOneProperty() {
        // One property per allOf entry — extreme split
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode allOf = schema.putArray("allOf");

        allOf.addObject().putObject("properties")
                .putObject("persons").put("type", "array");

        allOf.addObject().putObject("properties")
                .putObject("index")
                .put("type", "integer").put("minimum", 0)
                .put("description", "0-based index");

        allOf.addObject().putObject("properties")
                .putObject("total")
                .put("type", "integer").put("minimum", 1);

        allOf.addObject().putObject("properties")
                .putObject("count").put("type", "integer");

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_anyOf_propertiesInBranches() {
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode anyOf = schema.putArray("anyOf");

        // Branch A: array + index
        ObjectNode branchA = anyOf.addObject().putObject("properties");
        branchA.putObject("persons").put("type", "array");
        branchA.putObject("index")
                .put("type", "integer").put("minimum", 0)
                .put("description", "current page index");

        // Branch B: total + count
        ObjectNode branchB = anyOf.addObject().putObject("properties");
        branchB.putObject("total").put("type", "integer").put("minimum", 1);
        branchB.putObject("count").put("type", "integer");

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_oneOf_propertiesInBranches() {
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode oneOf = schema.putArray("oneOf");
        personsProps(oneOf.addObject().putObject("properties"));

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_nestedComposition_allOfInsideAnyOf() {
        // anyOf → allOf → properties
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode anyOf = schema.putArray("anyOf");
        ArrayNode allOf = anyOf.addObject().putArray("allOf");
        personsProps(allOf.addObject().putObject("properties"));

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_ifThenElse_propertiesInBranches() {
        ObjectNode schema = MAPPER.createObjectNode();
        // index + total in "then", count + persons in "else"
        ObjectNode thenProps = schema.putObject("then").putObject("properties");
        thenProps.putObject("index")
                .put("type", "integer").put("minimum", 0)
                .put("description", "0-based index");
        thenProps.putObject("total")
                .put("type", "integer").put("minimum", 1);

        ObjectNode elseProps = schema.putObject("else").putObject("properties");
        elseProps.putObject("count").put("type", "integer");
        elseProps.putObject("persons").put("type", "array");

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    // ── $defs / definitions reference resolution ─────────────────────────────

    @Test
    void analyze_defsWithRef_propertiesResolvedFromDefs() {
        // Top-level allOf with $refs into $defs
        ObjectNode schema = MAPPER.createObjectNode();

        // $defs
        ObjectNode defs = schema.putObject("$defs");
        defs.putObject("ArrayPart").putObject("properties")
                .putObject("persons").put("type", "array");
        ObjectNode pagPart = defs.putObject("PaginationPart").putObject("properties");
        pagPart.putObject("index")
                .put("type", "integer").put("minimum", 0)
                .put("description", "0-based page index");
        pagPart.putObject("total").put("type", "integer").put("minimum", 1);
        pagPart.putObject("count").put("type", "integer");

        // allOf with $refs
        ArrayNode allOf = schema.putArray("allOf");
        allOf.addObject().put("$ref", "#/$defs/ArrayPart");
        allOf.addObject().put("$ref", "#/$defs/PaginationPart");

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_definitionsKeyword_resolvedFromDefinitions() {
        // Same as above but uses legacy "definitions" instead of "$defs"
        ObjectNode schema = MAPPER.createObjectNode();

        ObjectNode definitions = schema.putObject("definitions");
        personsProps(definitions.putObject("Page").putObject("properties"));

        schema.putArray("allOf")
                .addObject().put("$ref", "#/definitions/Page");

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    // ── Mixed: top-level properties + allOf ──────────────────────────────────

    @Test
    void analyze_topLevelPropertiesMixedWithAllOf() {
        ObjectNode schema = MAPPER.createObjectNode();

        // persons + index at top level
        ObjectNode topProps = schema.putObject("properties");
        topProps.putObject("persons").put("type", "array");
        topProps.putObject("index")
                .put("type", "integer").put("minimum", 0)
                .put("description", "0-based index");

        // total + count via allOf
        ObjectNode allOfProps = schema.putArray("allOf")
                .addObject().putObject("properties");
        allOfProps.putObject("total").put("type", "integer").put("minimum", 1);
        allOfProps.putObject("count").put("type", "integer");

        assertPersonsSchema(SchemaAnalyzer.analyze(schema));
    }

    // ── Different field names ─────────────────────────────────────────────────

    @Test
    void analyze_customSchemaWithDifferentFieldNames() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.putArray("required")
                .add("orders").add("pageIndex").add("pageCount").add("itemCount");

        ObjectNode props = schema.putObject("properties");
        props.putObject("orders").put("type", "array");
        props.putObject("pageIndex")
                .put("type", "integer").put("minimum", 0)
                .put("description", "0-based current page index");
        props.putObject("pageCount").put("type", "integer").put("minimum", 1);
        props.putObject("itemCount").put("type", "integer");

        PaginationSchema ps = SchemaAnalyzer.analyze(schema);
        assertEquals("orders",    ps.getArrayFieldName());
        assertEquals("pageIndex", ps.getIndexFieldName());
        assertEquals("pageCount", ps.getTotalFieldName());
        assertEquals("itemCount", ps.getCountFieldName());
    }

    // ── collectProperties unit test ───────────────────────────────────────────

    @Test
    void collectProperties_traversesAllCompositionKeywords() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.putObject("properties").putObject("p1").put("type", "string");
        schema.putArray("allOf").addObject().putObject("properties")
                .putObject("p2").put("type", "string");
        schema.putArray("anyOf").addObject().putObject("properties")
                .putObject("p3").put("type", "string");
        schema.putArray("oneOf").addObject().putObject("properties")
                .putObject("p4").put("type", "string");
        schema.putObject("if").putObject("properties").putObject("p5").put("type", "string");
        schema.putObject("then").putObject("properties").putObject("p6").put("type", "string");
        schema.putObject("else").putObject("properties").putObject("p7").put("type", "string");

        Map<String, JsonNode> result = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, result);

        for (int i = 1; i <= 7; i++) {
            assertTrue(result.containsKey("p" + i), "p" + i + " should be collected");
        }
    }

    @Test
    void collectProperties_doesNotDescendIntoPropertyValues() {
        // "nested" is a property whose own sub-properties must NOT be collected
        ObjectNode schema = MAPPER.createObjectNode();
        ObjectNode props = schema.putObject("properties");
        ObjectNode nested = props.putObject("nested");
        nested.put("type", "object");
        nested.putObject("properties").putObject("deep").put("type", "string");

        Map<String, JsonNode> result = new LinkedHashMap<>();
        SchemaAnalyzer.collectProperties(schema, schema, result);

        assertTrue(result.containsKey("nested"), "'nested' must be collected");
        assertFalse(result.containsKey("deep"), "'deep' must not be collected (inside a property value)");
    }

    // ── Error cases ───────────────────────────────────────────────────────────

    /**
     * {@code input-person.schema.json} describes a single person with no array-typed property.
     * {@link SchemaAnalyzer} must reject it regardless of how properties are structured.
     */
    @Test
    void analyze_inputPersonSchema_throwsBecauseNoArrayField() throws Exception {
        JsonNode schema = loadClasspathSchema("schemas/input-person.schema.json");

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SchemaAnalyzer.analyze(schema));

        assertTrue(ex.getMessage().toLowerCase().contains("array"),
                "Exception should mention 'array', was: " + ex.getMessage());
    }

    @Test
    void analyze_throwsIfNoArrayField() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.putObject("properties")
                .putObject("age").put("type", "integer");

        assertThrows(IllegalArgumentException.class, () -> SchemaAnalyzer.analyze(schema));
    }

    @Test
    void analyze_throwsIfNoPropertiesAnywhere() {
        // Completely empty schema
        assertThrows(IllegalArgumentException.class,
                () -> SchemaAnalyzer.analyze(MAPPER.createObjectNode()));
    }

    @Test
    void analyze_throwsIfAmbiguousIndexField() {
        ObjectNode schema = MAPPER.createObjectNode();
        ObjectNode props = schema.putObject("properties");
        props.putObject("items").put("type", "array");
        props.putObject("pageIndex")
                .put("type", "integer").put("minimum", 0)
                .put("description", "0-based page index");
        props.putObject("currentIndex")
                .put("type", "integer").put("minimum", 0)
                .put("description", "Current index in the sequence");
        props.putObject("total")
                .put("type", "integer").put("minimum", 1);

        assertThrows(IllegalArgumentException.class, () -> SchemaAnalyzer.analyze(schema));
    }

    // ── loadSchema ────────────────────────────────────────────────────────────

    @Test
    void loadSchema_classpathResource_returnsJsonNode() throws Exception {
        JsonNode node = SchemaAnalyzer.loadSchema("schemas/csv-person-flat.schema.json");
        assertNotNull(node);
        assertTrue(node.has("required"), "loaded schema must have 'required'");
    }

    @Test
    void loadSchema_missingResource_throwsIllegalState() {
        assertThrows(IllegalStateException.class,
                () -> SchemaAnalyzer.loadSchema("schemas/does-not-exist.schema.json"));
    }

    // ── extractCsvColumns ─────────────────────────────────────────────────────

    @Test
    void extractCsvColumns_realSchema_returnsOrderedColumns() throws Exception {
        JsonNode schema = SchemaAnalyzer.loadSchema("schemas/csv-person-flat.schema.json");
        java.util.List<String> cols = SchemaAnalyzer.extractCsvColumns(schema);

        assertEquals(java.util.List.of("firstName", "lastName", "age", "address.street"), cols);
    }

    @Test
    void extractCsvColumns_orderMatchesRequiredArray() {
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode  req    = schema.putArray("required");
        req.add("z"); req.add("a"); req.add("m"); // intentional non-alphabetic order

        java.util.List<String> cols = SchemaAnalyzer.extractCsvColumns(schema);
        assertEquals(java.util.List.of("z", "a", "m"), cols, "column order must follow 'required' array");
    }

    @Test
    void extractCsvColumns_missingRequired_throws() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.putObject("properties").put("x", "string");

        assertThrows(IllegalArgumentException.class,
                () -> SchemaAnalyzer.extractCsvColumns(schema));
    }

    @Test
    void extractCsvColumns_emptyRequired_throws() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.putArray("required"); // empty array

        assertThrows(IllegalArgumentException.class,
                () -> SchemaAnalyzer.extractCsvColumns(schema));
    }

    @Test
    void extractCsvColumns_propertyNotInRequired_throws() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.putArray("required").add("a");
        schema.putObject("properties")
                .put("a", "string")
                .put("optionalField", "string"); // not in required

        assertThrows(IllegalArgumentException.class,
                () -> SchemaAnalyzer.extractCsvColumns(schema));
    }

    @Test
    void extractCsvColumns_allPropertiesRequired_succeeds() {
        ObjectNode schema = MAPPER.createObjectNode();
        ArrayNode req = schema.putArray("required");
        req.add("a"); req.add("b");
        ObjectNode props = schema.putObject("properties");
        props.putObject("a").put("type", "string");
        props.putObject("b").put("type", "integer");

        java.util.List<String> cols = SchemaAnalyzer.extractCsvColumns(schema);
        assertEquals(java.util.List.of("a", "b"), cols);
    }

    @Test
    void extractCsvColumns_noPropertiesBlock_allowedWithRequiredOnly() {
        // properties is optional in JSON Schema — required alone is sufficient
        ObjectNode schema = MAPPER.createObjectNode();
        schema.putArray("required").add("col1").add("col2");

        java.util.List<String> cols = SchemaAnalyzer.extractCsvColumns(schema);
        assertEquals(java.util.List.of("col1", "col2"), cols);
    }
}
