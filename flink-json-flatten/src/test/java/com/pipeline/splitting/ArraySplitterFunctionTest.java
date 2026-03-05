package com.pipeline.splitting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.PaginationSchema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link ArraySplitterFunction} using the Flink
 * {@link OneInputStreamOperatorTestHarness}.
 *
 * <p>Verifies splitting logic, correct pagination metadata, DLQ routing,
 * and schema-agnostic behaviour.
 */
class ArraySplitterFunctionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Default schema matching the {@code output-persons-paginated.schema.json} field names. */
    private static final PaginationSchema PERSONS_SCHEMA =
            new PaginationSchema("persons", "index", "total", "count");

    private ArraySplitterFunction function;

    @BeforeEach
    void setUp() {
        function = new ArraySplitterFunction("persons", 2, PERSONS_SCHEMA);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Build a JSON bytes payload with N simple person objects. */
    private static byte[] personsJson(int count) throws Exception {
        StringBuilder sb = new StringBuilder("{\"persons\":[");
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(",");
            sb.append("{\"id\":").append(i).append("}");
        }
        sb.append("]}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private Result run(ArraySplitterFunction fn, byte[] input) throws Exception {
        var harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(fn));
        harness.setup(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
                .createSerializer(new ExecutionConfig()));
        harness.open();
        harness.processElement(input, System.currentTimeMillis());
        List<byte[]> pages = harness.extractOutputValues();

        // getSideOutput returns ConcurrentLinkedQueue<StreamRecord<X>> in Flink 1.20
        Queue<?> rawDlq = harness.getSideOutput(ArraySplitterFunction.DLQ_TAG);
        List<DlqRecord> dlq = new ArrayList<>();
        if (rawDlq != null) {
            rawDlq.forEach(sr -> {
                Object val = ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) sr).getValue();
                dlq.add((DlqRecord) val);
            });
        }
        harness.close();
        return new Result(pages, dlq);
    }

    private Result run(byte[] input) throws Exception {
        return run(function, input);
    }

    /** Parse one page from the raw bytes output. */
    private static JsonNode parse(byte[] bytes) throws Exception {
        return MAPPER.readTree(bytes);
    }

    private record Result(List<byte[]> pages, List<DlqRecord> dlq) {}

    // ── Splitting tests ───────────────────────────────────────────────────────

    @Test
    void singlePage_arrayFitsInOnePage() throws Exception {
        // 3 persons, pageSize=2 → ceil(3/2)=2 pages; re-create with pageSize=10 for single page
        ArraySplitterFunction fn = new ArraySplitterFunction("persons", 10, PERSONS_SCHEMA);
        Result r = run(fn, personsJson(3));

        assertEquals(1, r.pages().size(), "exactly one page expected");
        assertTrue(r.dlq().isEmpty(), "no DLQ records expected");

        JsonNode page = parse(r.pages().get(0));
        assertEquals(0, page.get("index").asInt(),  "index must be 0");
        assertEquals(1, page.get("total").asInt(),  "total must be 1");
        assertEquals(3, page.get("count").asInt(),  "count must be 3");
        assertTrue(page.get("persons").isArray(),   "persons must be an array");
    }

    @Test
    void multiPage_correctPagination() throws Exception {
        // 5 persons, pageSize=2 → ceil(5/2) = 3 pages with counts [2, 2, 1]
        Result r = run(personsJson(5));

        assertEquals(3, r.pages().size(), "expected 3 pages for 5 items at pageSize=2");
        assertTrue(r.dlq().isEmpty());

        int[] expectedCounts = {2, 2, 1};
        for (int i = 0; i < 3; i++) {
            JsonNode page = parse(r.pages().get(i));
            assertEquals(i,                  page.get("index").asInt(), "page index");
            assertEquals(3,                  page.get("total").asInt(), "total pages");
            assertEquals(expectedCounts[i],  page.get("count").asInt(), "item count page " + i);
        }
    }

    @Test
    void lastPage_hasCorrectCount() throws Exception {
        // 5 items, pageSize=2 → last page has 1 item (5 % 2 = 1)
        Result r = run(personsJson(5));

        JsonNode lastPage = parse(r.pages().get(2));
        assertEquals(1, lastPage.get("count").asInt(), "last page must have 1 item");
        assertEquals(1, lastPage.get("persons").size(), "array size must match count");
    }

    @Test
    void allOutputsMatchOutputSchema() throws Exception {
        // Every page must carry all four schema fields
        Result r = run(personsJson(5));

        for (byte[] pageBytes : r.pages()) {
            JsonNode page = parse(pageBytes);
            assertTrue(page.has("persons"), "missing 'persons'");
            assertTrue(page.has("index"),   "missing 'index'");
            assertTrue(page.has("total"),   "missing 'total'");
            assertTrue(page.has("count"),   "missing 'count'");
        }
    }

    // ── DLQ routing ───────────────────────────────────────────────────────────

    @Test
    void emptyBytes_routedToDlq() throws Exception {
        Result r = run(new byte[0]);

        assertTrue(r.pages().isEmpty(), "no main output for empty bytes");
        assertFalse(r.dlq().isEmpty(),  "DLQ must receive the record");
    }

    @Test
    void missingArrayField_routedToDlq() throws Exception {
        // Valid JSON but no "persons" field
        byte[] input = "{\"data\":[{\"id\":1}]}".getBytes(StandardCharsets.UTF_8);
        Result r = run(input);

        assertTrue(r.pages().isEmpty(), "no main output when array field is missing");
        assertFalse(r.dlq().isEmpty(),  "DLQ must receive the record");
        assertTrue(r.dlq().get(0).getErrorMessage().contains("persons"),
                "error message should name the missing field");
    }

    // ── Schema-agnostic test ──────────────────────────────────────────────────

    @Test
    void differentSchema_personsThenOrders() throws Exception {
        // Schema with entirely different field names
        PaginationSchema ordersSchema =
                new PaginationSchema("orders", "pageIndex", "pageCount", "itemCount");
        ArraySplitterFunction fn =
                new ArraySplitterFunction("orders", 2, ordersSchema);

        byte[] input = "{\"orders\":[{\"id\":1},{\"id\":2},{\"id\":3}]}"
                .getBytes(StandardCharsets.UTF_8);
        Result r = run(fn, input);

        assertEquals(2, r.pages().size(), "ceil(3/2)=2 pages");
        assertTrue(r.dlq().isEmpty());

        JsonNode p0 = parse(r.pages().get(0));
        assertTrue(p0.has("orders"),    "output must use 'orders' as array field");
        assertTrue(p0.has("pageIndex"), "output must use 'pageIndex'");
        assertTrue(p0.has("pageCount"), "output must use 'pageCount'");
        assertTrue(p0.has("itemCount"), "output must use 'itemCount'");
        assertFalse(p0.has("persons"),  "'persons' must NOT appear when using orders schema");
        assertFalse(p0.has("index"),    "'index' must NOT appear when using orders schema");

        assertEquals(0, p0.get("pageIndex").asInt(), "first page index");
        assertEquals(2, p0.get("pageCount").asInt(), "total pages");
        assertEquals(2, p0.get("itemCount").asInt(), "items in first page");
        assertEquals(2, p0.get("orders").size());

        JsonNode p1 = parse(r.pages().get(1));
        assertEquals(1, p1.get("pageIndex").asInt(), "second page index");
        assertEquals(1, p1.get("itemCount").asInt(), "one item in last page");
    }
}
