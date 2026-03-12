package com.pipeline.processing;

import com.pipeline.config.PipelineConfig;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.deserialization.FlatteningDeserializer;
import com.pipeline.common.typeinfo.FlatRowSerializer;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link UpperCaseMapFunction}.
 *
 * <h2>Unit tests — {@link UpperCaseMapFunction#matches(String, String)}</h2>
 * Pure static method; no Flink harness required.
 *
 * <h2>Integration tests — Flink harness ({@link StreamMap})</h2>
 * Three groups:
 * <ol>
 *   <li>Netflix-specific director — exact pattern targets one index only.</li>
 *   <li>Netflix all directors — wildcard pattern {@code search_engines.*.imdb.director}
 *       uppercases every director across all array elements in one pass.</li>
 *   <li>Person names — exact and wildcard patterns on a flat persons array.</li>
 * </ol>
 */
class UpperCaseMapFunctionTest {

    // ── Fixtures ──────────────────────────────────────────────────────────────

    /** Three-engine Netflix JSON — enough to verify wildcard vs. exact behaviour. */
    private static final String NETFLIX_JSON = "{"
            + "\"application\":{\"name\":\"Netflix\",\"app_id\":\"netflix-test\",\"version\":\"v1\"},"
            + "\"search_engines\":["
            + "{\"category\":\"NetflixCategories\",\"name\":\"Action\",\"list\":[\"http://url0\"],\"pinned\":true,\"last_used\":0,"
            + "\"imdb\":{\"id\":\"tt1\",\"title\":\"Extraction\",\"year\":\"2020\",\"rated\":\"R\","
            + "\"genre\":\"Action, Thriller\",\"director\":\"Sam Hargrave\",\"actors\":\"Chris Hemsworth\",\"imdb_rating\":6.8}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Comedy\",\"list\":[\"http://url1\"],\"pinned\":false,\"last_used\":1,"
            + "\"imdb\":{\"id\":\"tt2\",\"title\":\"Deadpool 2\",\"year\":\"2018\",\"rated\":\"R\","
            + "\"genre\":\"Action, Adventure, Comedy\",\"director\":\"David Leitch\",\"actors\":\"Ryan Reynolds\",\"imdb_rating\":7.6}},"
            + "{\"category\":\"NetflixCategories\",\"name\":\"Sci-Fi\",\"list\":[\"http://url2\"],\"pinned\":false,\"last_used\":2,"
            + "\"imdb\":{\"id\":\"tt3\",\"title\":\"Mad Max: Fury Road\",\"year\":\"2015\",\"rated\":\"R\","
            + "\"genre\":\"Action, Adventure, Sci-Fi\",\"director\":\"George Miller\",\"actors\":\"Charlize Theron\",\"imdb_rating\":8.1}}]}";

    /** Two-person JSON — firstName, lastName, age only. */
    private static final String PERSONS_JSON = "{"
            + "\"persons\":["
            + "{\"firstName\":\"John\",\"lastName\":\"Doe\",\"age\":30},"
            + "{\"firstName\":\"Jane\",\"lastName\":\"Smith\",\"age\":28}]}";

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Flatten a JSON string into a single {@link Row} via {@link FlatteningDeserializer}.
     * The returned row is in named-field mode with dot-notation keys.
     */
    private static Row flattenJson(String json) throws Exception {
        byte[] bytes = json.strip().getBytes(StandardCharsets.UTF_8);
        var deserializer = new FlatteningDeserializer(new PipelineConfig.Builder().build());
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(deserializer));
        harness.setup(FlatRowSerializer.INSTANCE);
        harness.open();
        harness.processElement(bytes, System.currentTimeMillis());
        List<Row> rows = harness.extractOutputValues();
        harness.close();
        assertEquals(1, rows.size(), "expected a single flattened Row");
        return rows.get(0);
    }

    /**
     * Apply {@link UpperCaseMapFunction} (fallback mode — no vendor JAR) to {@code inputRow}
     * using the given {@code fieldKeys} patterns.
     */
    private static Row applyUpperCase(List<String> fieldKeys, Row inputRow) throws Exception {
        PipelineConfig config = new PipelineConfig.Builder()
                .uppercaseFieldKeys(fieldKeys)
                .build();
        UpperCaseMapFunction fn = new UpperCaseMapFunction(config);
        var harness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofPayload(inputRow), System.currentTimeMillis());
        List<ProcessedMessage> output = harness.extractOutputValues();
        harness.close();
        assertEquals(1, output.size(), "expected exactly one output Row");
        return output.get(0).getPayload();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Unit tests — matches()
    // ══════════════════════════════════════════════════════════════════════════

    @Test
    void matches_exactPattern_sameField_returnsTrue() {
        assertTrue(UpperCaseMapFunction.matches("person.name", "person.name"));
    }

    @Test
    void matches_exactPattern_differentField_returnsFalse() {
        assertFalse(UpperCaseMapFunction.matches("person.name", "person.age"));
    }

    @Test
    void matches_wildcardMiddleSegment_numericIndex_returnsTrue() {
        assertTrue(UpperCaseMapFunction.matches(
                "search_engines.*.imdb.director",
                "search_engines.0.imdb.director"));
        assertTrue(UpperCaseMapFunction.matches(
                "search_engines.*.imdb.director",
                "search_engines.4.imdb.director"));
    }

    @Test
    void matches_wildcardMiddleSegment_wrongTrailingField_returnsFalse() {
        assertFalse(UpperCaseMapFunction.matches(
                "search_engines.*.imdb.director",
                "search_engines.0.imdb.title"));
    }

    @Test
    void matches_wildcardPattern_segmentCountMismatch_returnsFalse() {
        // pattern has 4 parts; field has only 2
        assertFalse(UpperCaseMapFunction.matches(
                "search_engines.*.imdb.director",
                "application.name"));
    }

    @Test
    void matches_noWildcard_differentIndex_returnsFalse() {
        // exact pattern for index 0 must not match index 1
        assertFalse(UpperCaseMapFunction.matches(
                "search_engines.0.imdb.director",
                "search_engines.1.imdb.director"));
    }

    @Test
    void matches_wildcardAloneAsEntirePattern_matchesAnyTopLevelKey() {
        assertTrue(UpperCaseMapFunction.matches("*", "name"));
        assertTrue(UpperCaseMapFunction.matches("*", "category"));
        assertFalse(UpperCaseMapFunction.matches("*", "person.name")); // segment count differs
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Netflix — specific director (exact pattern, index 0)
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Exact pattern {@code "search_engines.0.imdb.director"} uppercases only the
     * director at index 0; the other two directors must remain unchanged.
     */
    @Test
    void netflix_exactPattern_uppercasesOnlyTargetDirector() throws Exception {
        Row inputRow = flattenJson(NETFLIX_JSON);
        Row out = applyUpperCase(List.of("search_engines.0.imdb.director"), inputRow);

        assertEquals("SAM HARGRAVE",  out.getField("search_engines.0.imdb.director"),
                "director at index 0 must be uppercased");
        assertEquals("David Leitch",  out.getField("search_engines.1.imdb.director"),
                "director at index 1 must be unchanged");
        assertEquals("George Miller", out.getField("search_engines.2.imdb.director"),
                "director at index 2 must be unchanged");
    }

    @Test
    void netflix_exactPattern_nonDirectorFieldsUnchanged() throws Exception {
        Row inputRow = flattenJson(NETFLIX_JSON);
        Row out = applyUpperCase(List.of("search_engines.0.imdb.director"), inputRow);

        assertEquals("Extraction",    out.getField("search_engines.0.imdb.title"),
                "title must not be touched");
        assertEquals("Netflix",       out.getField("application.name"),
                "application.name must not be touched");
        assertEquals("Action",        out.getField("search_engines.0.name"),
                "category name must not be touched");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Netflix — all directors (wildcard pattern)
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Wildcard pattern {@code "search_engines.*.imdb.director"} must uppercase
     * every director across all three array elements in a single pass.
     */
    @Test
    void netflix_wildcardPattern_uppercasesAllDirectors() throws Exception {
        Row inputRow = flattenJson(NETFLIX_JSON);
        Row out = applyUpperCase(List.of("search_engines.*.imdb.director"), inputRow);

        assertEquals("SAM HARGRAVE",  out.getField("search_engines.0.imdb.director"),
                "director 0 must be uppercased");
        assertEquals("DAVID LEITCH",  out.getField("search_engines.1.imdb.director"),
                "director 1 must be uppercased");
        assertEquals("GEORGE MILLER", out.getField("search_engines.2.imdb.director"),
                "director 2 must be uppercased");
    }

    @Test
    void netflix_wildcardPattern_nonDirectorFieldsUntouched() throws Exception {
        Row inputRow = flattenJson(NETFLIX_JSON);
        Row out = applyUpperCase(List.of("search_engines.*.imdb.director"), inputRow);

        assertEquals("Action, Thriller",          out.getField("search_engines.0.imdb.genre"),
                "genre must not be uppercased");
        assertEquals("Action, Adventure, Comedy", out.getField("search_engines.1.imdb.genre"),
                "genre must not be uppercased");
        assertEquals("Netflix",                   out.getField("application.name"),
                "application.name must not be uppercased");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Persons — firstName uppercase
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Exact pattern {@code "persons.0.firstName"} targets only the first person;
     * the second person's firstName and both lastNames must remain lowercase.
     */
    @Test
    void person_exactPattern_uppercasesOnlyFirstPersonName() throws Exception {
        Row inputRow = flattenJson(PERSONS_JSON);
        Row out = applyUpperCase(List.of("persons.0.firstName"), inputRow);

        assertEquals("JOHN", out.getField("persons.0.firstName"),
                "persons[0].firstName must be uppercased");
        assertEquals("Doe",  out.getField("persons.0.lastName"),
                "persons[0].lastName must not be touched");
        assertEquals("Jane", out.getField("persons.1.firstName"),
                "persons[1].firstName must not be touched");
    }

    /**
     * Wildcard pattern {@code "persons.*.firstName"} uppercases firstName for
     * every person while leaving lastName unchanged.
     */
    @Test
    void person_wildcardPattern_uppercasesAllFirstNames() throws Exception {
        Row inputRow = flattenJson(PERSONS_JSON);
        Row out = applyUpperCase(List.of("persons.*.firstName"), inputRow);

        assertEquals("JOHN", out.getField("persons.0.firstName"),
                "persons[0].firstName must be uppercased");
        assertEquals("JANE", out.getField("persons.1.firstName"),
                "persons[1].firstName must be uppercased");
        assertEquals("Doe",   out.getField("persons.0.lastName"),
                "persons[0].lastName must not be touched");
        assertEquals("Smith", out.getField("persons.1.lastName"),
                "persons[1].lastName must not be touched");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Multiple patterns in a single pass
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Two patterns supplied together: one exact (director at index 0) and one
     * top-level application name.  Both must be uppercased in a single map call.
     */
    @Test
    void multiplePatterns_allMatchingFieldsUppercased() throws Exception {
        Row inputRow = flattenJson(NETFLIX_JSON);
        Row out = applyUpperCase(
                List.of("search_engines.0.imdb.director", "application.name"),
                inputRow);

        assertEquals("SAM HARGRAVE", out.getField("search_engines.0.imdb.director"),
                "director 0 must be uppercased");
        assertEquals("NETFLIX",      out.getField("application.name"),
                "application.name must be uppercased");
        // Unmatched director must stay unchanged
        assertEquals("David Leitch", out.getField("search_engines.1.imdb.director"),
                "director 1 must not be touched");
    }

    /**
     * A wildcard pattern covering all actors and an exact pattern covering one title —
     * demonstrates that multiple heterogeneous patterns cooperate correctly.
     */
    @Test
    void multiplePatterns_wildcardActorsAndExactTitle() throws Exception {
        Row inputRow = flattenJson(NETFLIX_JSON);
        Row out = applyUpperCase(
                List.of("search_engines.*.imdb.actors", "search_engines.1.imdb.title"),
                inputRow);

        // All actors uppercased
        assertEquals("CHRIS HEMSWORTH", out.getField("search_engines.0.imdb.actors"));
        assertEquals("RYAN REYNOLDS",   out.getField("search_engines.1.imdb.actors"));
        assertEquals("CHARLIZE THERON", out.getField("search_engines.2.imdb.actors"));

        // Only title at index 1 uppercased; others unchanged
        assertEquals("DEADPOOL 2",       out.getField("search_engines.1.imdb.title"));
        assertEquals("Extraction",       out.getField("search_engines.0.imdb.title"));
        assertEquals("Mad Max: Fury Road", out.getField("search_engines.2.imdb.title"));
    }
}
