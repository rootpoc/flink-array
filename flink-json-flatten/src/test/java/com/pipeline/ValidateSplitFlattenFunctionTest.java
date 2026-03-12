package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import com.pipeline.common.PaginationSchema;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ValidateSplitFlattenFunction} and {@link InputSchemaAnalyzer}.
 *
 * <p>Covers schema analysis, happy-path splitting and flattening, null-handling
 * modes, DLQ routing for all failure cases, and schema-agnostic behaviour.
 */
class ValidateSplitFlattenFunctionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Netflix pagination schema — field names used throughout these tests. */
    private static final PaginationSchema NETFLIX_OUTPUT_SCHEMA =
            new PaginationSchema("search_engines", "index", "total", "count");

    /** Validation rules matching {@code input-netflix-categories.schema.json}. */
    private static final InputSchemaInfo NETFLIX_INPUT_SCHEMA = new InputSchemaInfo(
            "search_engines",
            List.of("application", "search_engines"),
            List.of("category", "name", "list", "pinned", "last_used"));

    // ── Fixtures ──────────────────────────────────────────────────────────────

    /** Minimal valid Netflix JSON with {@code n} search-engine items. */
    private static byte[] netflixJson(int n) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},");
        sb.append("\"search_engines\":[");
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(",");
            sb.append("{\"category\":\"Cat\",\"name\":\"Engine").append(i)
              .append("\",\"list\":[\"url").append(i)
              .append("\"],\"pinned\":true,\"last_used\":").append(i).append("}");
        }
        sb.append("]}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Single engine with an {@code imdb} nested object. */
    private static final String NETFLIX_WITH_IMDB = "{"
            + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"id\",\"version\":\"v1\"},"
            + "\"search_engines\":[{"
            + "\"category\":\"Movies\",\"name\":\"IMDB\","
            + "\"list\":[\"url0\",\"url1\"],\"pinned\":true,\"last_used\":100,"
            + "\"imdb\":{\"id\":\"tt1\",\"title\":\"Film\",\"year\":\"2020\",\"rated\":\"PG\","
            + "\"genre\":\"Action\",\"director\":\"Jane Doe\",\"actors\":\"Actor A\","
            + "\"imdb_rating\":7.5}}]}";

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static JsonNode loadClasspathSchema(String path) throws Exception {
        try (InputStream is = ValidateSplitFlattenFunctionTest.class
                .getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Schema not found: " + path);
            return MAPPER.readTree(is);
        }
    }

    private Result run(InputSchemaInfo inputSchema,
                       PaginationSchema outputSchema,
                       int pageSize,
                       NullHandling nullHandling,
                       byte[] input) throws Exception {
        ValidateSplitFlattenFunction fn =
                new ValidateSplitFlattenFunction(inputSchema, outputSchema, pageSize, nullHandling);
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofValue(input), System.currentTimeMillis());
        List<Row> rows = harness.<ProcessedMessage>extractOutputValues()
                .stream().map(ProcessedMessage::getPayload).collect(Collectors.toList());

        Queue<?> rawDlq = harness.getSideOutput(ValidateSplitFlattenFunction.DLQ_TAG);
        List<DlqRecord> dlq = new ArrayList<>();
        if (rawDlq != null) {
            rawDlq.forEach(sr ->
                    dlq.add((DlqRecord) ((StreamRecord<?>) sr).getValue()));
        }
        harness.close();
        return new Result(rows, dlq);
    }

    private Result run(byte[] input) throws Exception {
        return run(NETFLIX_INPUT_SCHEMA, NETFLIX_OUTPUT_SCHEMA, 10, NullHandling.INCLUDE, input);
    }

    private Result run(int pageSize, byte[] input) throws Exception {
        return run(NETFLIX_INPUT_SCHEMA, NETFLIX_OUTPUT_SCHEMA, pageSize, NullHandling.INCLUDE, input);
    }

    private static final class Result {
        private final List<Row> rows;
        private final List<DlqRecord> dlq;

        private Result(List<Row> rows, List<DlqRecord> dlq) {
            this.rows = rows;
            this.dlq = dlq;
        }

        List<Row> rows() { return rows; }

        List<DlqRecord> dlq() { return dlq; }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // InputSchemaAnalyzer
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void analyzeNetflixSchema_extractsArrayFieldName() throws Exception {
        JsonNode schema = loadClasspathSchema("schemas/input-netflix-categories.schema.json");
        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertEquals("search_engines", info.getArrayFieldName());
    }

    @Test
    void analyzeNetflixSchema_extractsRequiredTopLevelFields() throws Exception {
        JsonNode schema = loadClasspathSchema("schemas/input-netflix-categories.schema.json");
        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertTrue(info.getRequiredFields().containsAll(List.of("application", "search_engines")));
    }

    @Test
    void analyzeNetflixSchema_extractsRequiredItemFields() throws Exception {
        JsonNode schema = loadClasspathSchema("schemas/input-netflix-categories.schema.json");
        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertTrue(info.getRequiredItemFields().containsAll(
                List.of("category", "name", "list", "pinned", "last_used")));
    }

    @Test
    void analyze_noArrayField_returnsNullArrayFieldName() {
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        schema.putObject("properties").putObject("name").put("type", "string");
        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertNull(info.getArrayFieldName(), "flat schema must have null arrayFieldName");
    }

    @Test
    void analyze_schemaWithArrayField_extractsItemRequired() throws Exception {
        // Explicit "with array" case: items sub-schema carries its own required list
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        schema.putArray("required").add("items");
        var props = schema.putObject("properties");
        var arr = props.putObject("items");
        arr.put("type", "array");
        var itemsSchema = arr.putObject("items");
        itemsSchema.putArray("required").add("id").add("value");

        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertEquals("items", info.getArrayFieldName());
        assertTrue(info.getRequiredItemFields().containsAll(List.of("id", "value")));
    }

    @Test
    void analyze_schemaWithArrayFieldButNoItemsRequired_emptyItemFields() throws Exception {
        // "with array" but the items sub-schema has no required list → empty item-level rules
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        schema.putObject("properties").putObject("records").put("type", "array");

        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertEquals("records", info.getArrayFieldName());
        assertTrue(info.getRequiredItemFields().isEmpty(),
                "no items.required in schema → empty required item fields");
    }

    @Test
    void analyze_emptySchema_returnsNullArrayFieldAndEmptyLists() {
        ObjectMapper m = new ObjectMapper();
        InputSchemaInfo info = InputSchemaInfo.analyze(m.createObjectNode());
        assertNull(info.getArrayFieldName());
        assertTrue(info.getRequiredFields().isEmpty());
        assertTrue(info.getRequiredItemFields().isEmpty());
    }

    @Test
    void analyze_schemaWithOnlyNonArrayFields_returnsNullArrayFieldName() {
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        var props = schema.putObject("properties");
        props.putObject("name").put("type", "string");
        props.putObject("meta").put("type", "object");
        props.putObject("count").put("type", "integer");
        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertNull(info.getArrayFieldName(), "schema with no array-typed property must have null arrayFieldName");
    }

    // ── Composition keywords ──────────────────────────────────────────────────

    @Test
    void analyze_allOf_unionOfAllBranchesRequired() {
        // allOf: all branches must match → required fields from every branch are required
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        var allOf = schema.putArray("allOf");
        allOf.addObject().putArray("required").add("firstName").add("lastName");
        allOf.addObject().putArray("required").add("age");

        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertTrue(info.getRequiredFields().containsAll(List.of("firstName", "lastName", "age")),
                "allOf: all branches' required fields must be collected");
    }

    @Test
    void analyze_anyOf_intersectionOfAllBranchesRequired() {
        // anyOf: at least one must match → only fields required in ALL branches are guaranteed
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        var anyOf = schema.putArray("anyOf");
        anyOf.addObject().putArray("required").add("id").add("name");
        anyOf.addObject().putArray("required").add("id").add("email");

        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertTrue(info.getRequiredFields().contains("id"),
                "anyOf: 'id' required in all branches must be in result");
        assertFalse(info.getRequiredFields().contains("name"),
                "anyOf: 'name' required in only one branch must NOT be in result");
        assertFalse(info.getRequiredFields().contains("email"),
                "anyOf: 'email' required in only one branch must NOT be in result");
    }

    @Test
    void analyze_oneOf_intersectionOfAllBranchesRequired() {
        // oneOf: exactly one must match → same intersection logic as anyOf
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        var oneOf = schema.putArray("oneOf");
        oneOf.addObject().putArray("required").add("type").add("cardNumber");
        oneOf.addObject().putArray("required").add("type").add("iban");

        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertTrue(info.getRequiredFields().contains("type"),
                "oneOf: 'type' required in all branches must be in result");
        assertFalse(info.getRequiredFields().contains("cardNumber"),
                "oneOf: 'cardNumber' required in only one branch must NOT be in result");
        assertFalse(info.getRequiredFields().contains("iban"),
                "oneOf: 'iban' required in only one branch must NOT be in result");
    }

    @Test
    void analyze_allOfAndDirectRequired_combined() {
        // direct required + allOf: both sources contribute to the union
        ObjectMapper m = new ObjectMapper();
        var schema = m.createObjectNode();
        schema.putArray("required").add("tenantId");
        schema.putArray("allOf")
              .addObject().putArray("required").add("firstName").add("lastName");

        InputSchemaInfo info = InputSchemaInfo.analyze(schema);
        assertTrue(info.getRequiredFields().containsAll(List.of("tenantId", "firstName", "lastName")),
                "direct required + allOf required must all appear");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Happy path — splitting and pagination
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void singlePage_emitsOneRow() throws Exception {
        Result r = run(netflixJson(3));
        assertEquals(1, r.rows().size(), "3 items within pageSize=10 → one page");
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void singlePage_paginationFieldsCorrect() throws Exception {
        Result r = run(netflixJson(3));
        Row row = r.rows().get(0);
        assertEquals(0, row.getField("index"), "index must be 0");
        assertEquals(1, row.getField("total"), "total must be 1");
        assertEquals(3, row.getField("count"), "count must match item count");
    }

    @Test
    void multiplePages_emitsOneRowPerPage() throws Exception {
        Result r = run(2, netflixJson(5));
        assertEquals(3, r.rows().size(), "ceil(5/2) = 3 pages");
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void multiplePages_paginationMetadataCorrectPerPage() throws Exception {
        Result r = run(2, netflixJson(5));
        int[] expectedCounts = {2, 2, 1};
        for (int p = 0; p < 3; p++) {
            Row row = r.rows().get(p);
            assertEquals(p,                 row.getField("index"), "index page " + p);
            assertEquals(3,                 row.getField("total"), "total pages");
            assertEquals(expectedCounts[p], row.getField("count"), "count page " + p);
        }
    }

    @Test
    void lastPage_hasCorrectItemCount() throws Exception {
        Result r = run(2, netflixJson(5));
        Row last = r.rows().get(2);
        assertEquals(1, last.getField("count"), "last page has 1 item (5 % 2 = 1)");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Happy path — flattening
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void itemFields_flattenedWithDotNotationPrefix() throws Exception {
        Result r = run(netflixJson(1));
        Row row = r.rows().get(0);
        assertEquals("Engine0", row.getField("search_engines.0.name"),  "name");
        assertEquals("Cat",     row.getField("search_engines.0.category"), "category");
        assertEquals(true,      row.getField("search_engines.0.pinned"),   "pinned");
        assertEquals(0L,         row.getField("search_engines.0.last_used"),"last_used");
    }

    @Test
    void nestedImdb_flattenedCorrectly() throws Exception {
        Result r = run(NETFLIX_WITH_IMDB.getBytes(StandardCharsets.UTF_8));
        Row row = r.rows().get(0);
        assertEquals("Jane Doe", row.getField("search_engines.0.imdb.director"));
        assertEquals("Film",     row.getField("search_engines.0.imdb.title"));
        assertEquals(7.5,        row.getField("search_engines.0.imdb.imdb_rating"));
    }

    @Test
    void listArray_flattenedWithIndex() throws Exception {
        Result r = run(NETFLIX_WITH_IMDB.getBytes(StandardCharsets.UTF_8));
        Row row = r.rows().get(0);
        assertEquals("url0", row.getField("search_engines.0.list.0"));
        assertEquals("url1", row.getField("search_engines.0.list.1"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Null handling
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void nullHandling_include_nullKeptInRow() throws Exception {
        String json = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"category\":\"C\",\"name\":\"E\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,\"extra\":null}] }";
        Result r = run(NETFLIX_INPUT_SCHEMA, NETFLIX_OUTPUT_SCHEMA, 10, NullHandling.INCLUDE,
                json.getBytes(StandardCharsets.UTF_8));
        Row row = r.rows().get(0);
        // field must exist and be null
        assertNull(row.getField("search_engines.0.extra"), "null field must be kept");
    }

    @Test
    void nullHandling_exclude_nullFieldOmitted() throws Exception {
        String json = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"category\":\"C\",\"name\":\"E\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,\"extra\":null}] }";
        Result r = run(NETFLIX_INPUT_SCHEMA, NETFLIX_OUTPUT_SCHEMA, 10, NullHandling.EXCLUDE,
                json.getBytes(StandardCharsets.UTF_8));
        Row row = r.rows().get(0);
        var names = row.getFieldNames(false);
        assertFalse(names != null && names.contains("search_engines.0.extra"),
                "null field must be excluded");
    }

    @Test
    void nullHandling_replaceEmptyString_nullBecomesEmpty() throws Exception {
        String json = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v1\"},"
                + "\"search_engines\":[{\"category\":\"C\",\"name\":\"E\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,\"extra\":null}] }";
        Result r = run(NETFLIX_INPUT_SCHEMA, NETFLIX_OUTPUT_SCHEMA, 10,
                NullHandling.REPLACE_EMPTY_STRING,
                json.getBytes(StandardCharsets.UTF_8));
        Row row = r.rows().get(0);
        assertEquals("", row.getField("search_engines.0.extra"),
                "null field must become empty string");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DLQ routing
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void emptyBytes_routedToDlq() throws Exception {
        Result r = run(new byte[0]);
        assertTrue(r.rows().isEmpty());
        assertFalse(r.dlq().isEmpty());
    }

    @Test
    void malformedJson_routedToDlq() throws Exception {
        Result r = run("not-valid-json".getBytes(StandardCharsets.UTF_8));
        assertTrue(r.rows().isEmpty());
        assertFalse(r.dlq().isEmpty());
    }

    @Test
    void missingTopLevelField_application_routedToDlq() throws Exception {
        byte[] input = "{\"search_engines\":[]}".getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        assertFalse(r.dlq().isEmpty());
        assertTrue(r.dlq().get(0).getErrorMessage().contains("application"),
                "error must mention the missing field");
    }

    @Test
    void missingTopLevelField_searchEngines_routedToDlq() throws Exception {
        byte[] input = "{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"}}"
                .strip().getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        assertFalse(r.dlq().isEmpty());
        assertTrue(r.dlq().get(0).getErrorMessage().contains("search_engines"),
                "error must mention the missing field");
    }

    @Test
    void missingRequiredItemField_routedToDlq() throws Exception {
        // missing "category" in the single item
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[{\"name\":\"E\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        assertFalse(r.dlq().isEmpty());
        assertTrue(r.dlq().get(0).getErrorMessage().contains("category"),
                "error must mention the missing item field");
    }

    @Test
    void missingItemField_errorMessageMentionsIndex() throws Exception {
        // first item valid, second item missing "name"
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":["
                + "{\"category\":\"C\",\"name\":\"E\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0},"
                + "{\"category\":\"C\",\"list\":[\"u\"],\"pinned\":false,\"last_used\":1}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        String msg = r.dlq().get(0).getErrorMessage();
        assertTrue(msg.contains("[1]"), "error must identify item index 1: " + msg);
    }

    @Test
    void emptyArray_emitsNoRows() throws Exception {
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty(), "empty array must produce zero pages");
        assertTrue(r.dlq().isEmpty(),  "empty array is not a validation failure");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // With array — explicit structural scenarios
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void withArray_singleItem_rowContainsArrayFieldPrefix() throws Exception {
        // The Row must use the output schema's array field name as the prefix
        Result r = run(netflixJson(1));
        Row row = r.rows().get(0);
        var names = row.getFieldNames(false);
        assertNotNull(names);
        assertTrue(names.stream().anyMatch(n -> n.startsWith("search_engines.")),
                "flattened item fields must be prefixed with the array field name");
    }

    @Test
    void withArray_multipleItems_eachItemIndexedInRow() throws Exception {
        // Items at index 0 and 1 must produce separate prefixed keys
        Result r = run(netflixJson(2));
        Row row = r.rows().get(0);
        assertNotNull(row.getField("search_engines.0.name"), "item 0 must be present");
        assertNotNull(row.getField("search_engines.1.name"), "item 1 must be present");
    }

    @Test
    void withArray_nestedSubArray_flattenedWithNumericIndex() throws Exception {
        // The 'list' sub-array inside each item must be indexed: list.0, list.1 …
        Result r = run(NETFLIX_WITH_IMDB.getBytes(StandardCharsets.UTF_8));
        Row row = r.rows().get(0);
        assertNotNull(row.getField("search_engines.0.list.0"), "list[0] must be flattened");
        assertNotNull(row.getField("search_engines.0.list.1"), "list[1] must be flattened");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Without array — field is present but wrong type or entirely absent
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void withoutArray_searchEnginesIsObject_notArray_routedToDlq() throws Exception {
        // search_engines is an object {…}, not an array […]
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":{\"name\":\"Engine\"}}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty(), "non-array value must produce no output rows");
        assertFalse(r.dlq().isEmpty(), "non-array value must be routed to DLQ");
    }

    @Test
    void withoutArray_searchEnginesIsString_routedToDlq() throws Exception {
        // search_engines is a plain string — clearly wrong type
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":\"not-an-array\"}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void withoutArray_searchEnginesIsNull_routedToDlq() throws Exception {
        // search_engines is explicitly null
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":null}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void withoutArray_searchEnginesFieldMissing_routedToDlq() throws Exception {
        // Top-level required field search_engines is absent entirely
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"}}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        assertFalse(r.dlq().isEmpty());
        assertTrue(r.dlq().get(0).getErrorMessage().contains("search_engines"));
    }

    @Test
    void withoutArray_flatJsonWithNoArrayAtAll_routedToDlq() throws Exception {
        // Completely flat message — no array field anywhere
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"category\":\"C\",\"name\":\"E\"}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty(), "flat message without array field must produce no output");
        assertFalse(r.dlq().isEmpty(), "flat message must be routed to DLQ");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Missing required fields — comprehensive
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void missingRequired_allTopLevelFieldsMissing_errorListsAll() throws Exception {
        byte[] input = "{}".getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty());
        String msg = r.dlq().get(0).getErrorMessage();
        assertTrue(msg.contains("application"),    "error must mention 'application'");
        assertTrue(msg.contains("search_engines"), "error must mention 'search_engines'");
    }

    @Test
    void missingRequired_oneTopLevelFieldMissing_routedToDlq() throws Exception {
        // Only "search_engines" present; "application" missing — partial is still invalid
        byte[] input = ("{\"search_engines\":[{\"category\":\"C\",\"name\":\"E\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":0}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertFalse(r.dlq().isEmpty());
        assertTrue(r.dlq().get(0).getErrorMessage().contains("application"));
    }

    @Test
    void missingRequired_itemMissingMultipleFields_errorListsAll() throws Exception {
        // Item has only "category" — all other required fields missing
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[{\"category\":\"C\"}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        String msg = r.dlq().get(0).getErrorMessage();
        assertTrue(msg.contains("name"),      "error must mention 'name'");
        assertTrue(msg.contains("list"),      "error must mention 'list'");
        assertTrue(msg.contains("pinned"),    "error must mention 'pinned'");
        assertTrue(msg.contains("last_used"), "error must mention 'last_used'");
    }

    @Test
    void missingRequired_firstItemValidSecondItemInvalid_entireMessageToDlq() throws Exception {
        // Validation checks ALL items — one bad item rejects the whole message
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":["
                + "{\"category\":\"C\",\"name\":\"E\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0},"
                + "{\"category\":\"C\",\"list\":[\"u\"],\"pinned\":false,\"last_used\":1}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertTrue(r.rows().isEmpty(), "one invalid item must reject the entire message");
        assertFalse(r.dlq().isEmpty());
    }

    @Test
    void missingRequired_schemaWithNoItemRequirements_anyItemContentIsValid() throws Exception {
        // Schema that requires no item fields → any item content passes
        InputSchemaInfo noItemReqs = new InputSchemaInfo(
                "search_engines",
                List.of("application", "search_engines"),
                List.of());
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[{\"anything\":\"goes\"}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(noItemReqs, NETFLIX_OUTPUT_SCHEMA, 10, NullHandling.INCLUDE, input);
        assertFalse(r.rows().isEmpty(), "message must pass when schema has no item requirements");
        assertTrue(r.dlq().isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Additional (extra) fields — must be accepted, not cause DLQ
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void extraFields_atTopLevel_flowContinues() throws Exception {
        // Extra top-level fields beyond schema requirements must not cause DLQ
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[{\"category\":\"C\",\"name\":\"E\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":0}],"
                + "\"extra_field\":\"should-not-matter\",\"another_extra\":42}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertFalse(r.rows().isEmpty(), "extra top-level fields must not cause DLQ");
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void extraFields_insideItem_flowContinues() throws Exception {
        // Items with extra fields beyond required ones must not cause DLQ
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[{\"category\":\"C\",\"name\":\"E\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,"
                + "\"unexpected_field\":\"value\",\"score\":9.9}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        assertFalse(r.rows().isEmpty(), "extra item fields must not cause DLQ");
        assertTrue(r.dlq().isEmpty());
    }

    @Test
    void extraFields_insideItem_requiredFieldsStillPresentInRow() throws Exception {
        // Extra fields must not displace required fields in the output Row
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"search_engines\":[{\"category\":\"Movies\",\"name\":\"Engine\","
                + "\"list\":[\"u\"],\"pinned\":true,\"last_used\":5,\"bonus\":\"extra\"}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(input);
        Row row = r.rows().get(0);
        assertEquals("Movies", row.getField("search_engines.0.category"));
        assertEquals("Engine", row.getField("search_engines.0.name"));
        assertEquals(true,     row.getField("search_engines.0.pinned"));
        assertEquals(5L,        row.getField("search_engines.0.last_used"));
    }

    @Test
    void extraFields_extraItemsAndTopLevel_allItemsPaged() throws Exception {
        // Both top-level extras and item extras present — paging must still work
        byte[] input = ("{\"application\":{\"name\":\"N\",\"app_id\":\"id\",\"version\":\"v\"},"
                + "\"metadata\":{\"source\":\"test\"},"
                + "\"search_engines\":["
                + "{\"category\":\"C\",\"name\":\"E0\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":0,\"rank\":1},"
                + "{\"category\":\"C\",\"name\":\"E1\",\"list\":[\"u\"],\"pinned\":false,\"last_used\":1,\"rank\":2},"
                + "{\"category\":\"C\",\"name\":\"E2\",\"list\":[\"u\"],\"pinned\":true,\"last_used\":2,\"rank\":3}]}")
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(2, input);  // pageSize=2 → 2 pages
        assertEquals(2, r.rows().size(), "paging must still produce 2 pages");
        assertTrue(r.dlq().isEmpty());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Schema-agnostic
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void outputSchemaFieldNames_usedForPaginationFields() throws Exception {
        PaginationSchema customSchema =
                new PaginationSchema("search_engines", "pageIdx", "pageTotal", "pageCount");
        Result r = run(NETFLIX_INPUT_SCHEMA, customSchema, 10, NullHandling.INCLUDE,
                netflixJson(3));
        Row row = r.rows().get(0);
        assertNotNull(row.getField("pageIdx"),   "custom index field must be present");
        assertNotNull(row.getField("pageTotal"), "custom total field must be present");
        assertNotNull(row.getField("pageCount"), "custom count field must be present");
        assertNull(row.getField("index"),  "default 'index' must not appear");
        assertNull(row.getField("total"),  "default 'total' must not appear");
        assertNull(row.getField("count"),  "default 'count' must not appear");
    }
}
