package com.pipeline;

import com.pipeline.config.PipelineConfig;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.processing.UpperCaseMapFunction;
import com.pipeline.deserialization.FlatteningDeserializer;
import com.pipeline.common.typeinfo.FlatRowSerializer;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Diagnostic test that prints the full contents of the {@link Row} produced when
 * the Netflix categories JSON is deserialized by {@link FlatteningDeserializer}.
 *
 * <p>Run this test to inspect the dot-notation field names and value types that
 * the pipeline works with downstream (UpperCaseMapFunction, ReconstructSerializer, etc.).
 *
 * <p>The test always passes — it is purely informational.
 */
class NetflixRowInspectionTest {

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Row applyUpperCase(List<String> fieldKeys, Row inputRow) throws Exception {
        PipelineConfig config = new PipelineConfig.Builder()
                .uppercaseFieldKeys(fieldKeys)
                .build();
        UpperCaseMapFunction fn = new UpperCaseMapFunction(config);
        var harness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(fn));
        harness.setup(ProcessedMessageSerializer.INSTANCE);
        harness.open();
        harness.processElement(ProcessedMessage.ofPayload(inputRow), System.currentTimeMillis());
        List<ProcessedMessage> out = harness.extractOutputValues();
        harness.close();
        assertEquals(1, out.size());
        return out.get(0).getPayload();
    }

    private static List<Row> flattenBytes(byte[] input) throws Exception {
        var deserializer = new FlatteningDeserializer(
                new PipelineConfig.Builder().build());
        var harness = new OneInputStreamOperatorTestHarness<>(
                new ProcessOperator<>(deserializer));
        harness.setup(FlatRowSerializer.INSTANCE);
        harness.open();
        harness.processElement(input, System.currentTimeMillis());
        List<Row> rows = harness.extractOutputValues();
        harness.close();
        return rows;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /**
     * Deserializes the Netflix JSON into a single flat {@link Row} and prints every
     * field name + value + Java type, sorted alphabetically by field name.
     *
     * <p>Example output:
     * <pre>
     * ╔══════════════════════════════════════════════════════════════════════════════════╗
     * ║  Netflix flat Row — 73 fields                                                  ║
     * ╠══════════════════════════════════════════╤═════════════════════════╤════════════╣
     * ║  Field                                   │  Value                  │  Java type ║
     * ╠══════════════════════════════════════════╪═════════════════════════╪════════════╣
     * ║  application.app_id                      │  netflix-categories@... │  String    ║
     * ║  application.name                        │  Netflix Categories     │  String    ║
     * ...
     * </pre>
     */
    @Test
    void printNetflixFlatRow_allFields() throws Exception {
        List<Row> rows = flattenBytes(NetflixCategoriesTest.NETFLIX_BYTES);
        assertEquals(1, rows.size(), "expected exactly one flattened Row");

        Row row = rows.get(0);
        Set<String> fieldNames = row.getFieldNames(false);
        assertNotNull(fieldNames, "Row must be in named-field mode");

        // Sort alphabetically for readability
        TreeMap<String, Object> sorted = new TreeMap<>();
        for (String name : fieldNames) {
            sorted.put(name, row.getField(name));
        }

        // ── Pretty-print ───────────────────────────────────────────────────────
        int nameWidth  = sorted.keySet().stream().mapToInt(String::length).max().orElse(20) + 2;
        int valueWidth = sorted.values().stream()
                .mapToInt(v -> v == null ? 4 : String.valueOf(v).length())
                .max().orElse(20);
        valueWidth = Math.min(valueWidth, 55) + 2; // cap long URLs

        String border = "═".repeat(nameWidth + 2) + "╪" +
                        "═".repeat(valueWidth + 2) + "╪" +
                        "═".repeat(12);

        System.out.println();
        System.out.println("╔" + "═".repeat(nameWidth + valueWidth + 18) + "╗");
        System.out.printf("║  Netflix flat Row — %d fields%s║%n",
                sorted.size(),
                " ".repeat(nameWidth + valueWidth + 18 - 22 - String.valueOf(sorted.size()).length()));
        System.out.println("╠" + border + "╣");
        System.out.printf("║  %-" + nameWidth + "s│  %-" + valueWidth + "s│  %-10s║%n",
                "Field", "Value", "Java type");
        System.out.println("╠" + border + "╣");

        for (var entry : sorted.entrySet()) {
            String name  = entry.getKey();
            Object value = entry.getValue();
            String valueStr = value == null ? "null" : String.valueOf(value);
            if (valueStr.length() > valueWidth) {
                valueStr = valueStr.substring(0, valueWidth - 3) + "...";
            }
            String typeName = value == null ? "null"
                    : value.getClass().getSimpleName();

            System.out.printf("║  %-" + nameWidth + "s│  %-" + valueWidth + "s│  %-10s║%n",
                    name, valueStr, typeName);
        }

        System.out.println("╚" + "═".repeat(nameWidth + 2) + "╧" +
                           "═".repeat(valueWidth + 2) + "╧" +
                           "═".repeat(12) + "╝");
        System.out.println();
    }

    /**
     * Same as above but groups fields by search-engine index (0–4) for a
     * cleaner view of the per-engine structure.
     */
    @Test
    void printNetflixFlatRow_groupedByEngine() throws Exception {
        List<Row> rows = flattenBytes(NetflixCategoriesTest.NETFLIX_BYTES);
        Row row = rows.get(0);
        Set<String> fieldNames = row.getFieldNames(false);
        assertNotNull(fieldNames);

        System.out.println();
        System.out.println("=== Netflix flat Row — grouped by search engine ===");
        System.out.println();

        // Application section
        System.out.println("── application ──────────────────────────────────");
        fieldNames.stream()
                .filter(n -> n.startsWith("application."))
                .sorted()
                .forEach(n -> System.out.printf("  %-45s = %s%n", n, row.getField(n)));

        // Per-engine sections
        for (int i = 0; i < 5; i++) {
            final int idx = i;
            String prefix = "search_engines." + i + ".";
            boolean hasAny = fieldNames.stream().anyMatch(n -> n.startsWith(prefix));
            if (!hasAny) break;

            System.out.println();
            System.out.println("── search_engines[" + i + "] ──────────────────────────────");
            fieldNames.stream()
                    .filter(n -> n.startsWith(prefix))
                    .sorted()
                    .forEach(n -> System.out.printf("  %-45s = %s  [%s]%n",
                            n,
                            row.getField(n),
                            row.getField(n) == null ? "null"
                                    : row.getField(n).getClass().getSimpleName()));
        }
        System.out.println();
    }

    /**
     * Flattens the Netflix JSON, applies {@link UpperCaseMapFunction} with the wildcard
     * pattern {@code "search_engines.*.imdb.director"}, then prints the Row grouped by
     * engine — identical layout to {@link #printNetflixFlatRow_groupedByEngine()} so the
     * before/after difference is easy to spot.
     *
     * <p>Directors appear in ALL CAPS; every other field is unchanged.
     */
    @Test
    void printNetflixFlatRow_allDirectorsUppercased() throws Exception {
        // Step 1: flatten
        List<Row> rows = flattenBytes(NetflixCategoriesTest.NETFLIX_BYTES);
        assertEquals(1, rows.size());
        Row flatRow = rows.get(0);

        // Step 2: uppercase all directors
        Row out = applyUpperCase(List.of("search_engines.*.imdb.director"), flatRow);

        Set<String> fieldNames = out.getFieldNames(false);
        assertNotNull(fieldNames);

        System.out.println();
        System.out.println("=== Netflix flat Row — all directors uppercased ===");
        System.out.println("    pattern: \"search_engines.*.imdb.director\"");
        System.out.println();

        // Application section (unchanged)
        System.out.println("── application ──────────────────────────────────");
        fieldNames.stream()
                .filter(n -> n.startsWith("application."))
                .sorted()
                .forEach(n -> System.out.printf("  %-45s = %s%n", n, out.getField(n)));

        // Per-engine sections
        for (int i = 0; i < 5; i++) {
            String prefix = "search_engines." + i + ".";
            boolean hasAny = fieldNames.stream().anyMatch(n -> n.startsWith(prefix));
            if (!hasAny) break;

            System.out.println();
            System.out.println("── search_engines[" + i + "] ──────────────────────────────");
            fieldNames.stream()
                    .filter(n -> n.startsWith(prefix))
                    .sorted()
                    .forEach(n -> {
                        Object value = out.getField(n);
                        boolean changed = n.endsWith(".imdb.director");
                        System.out.printf("  %-45s = %-40s%s%n",
                                n,
                                value,
                                changed ? "  ◄ uppercased" : "");
                    });
        }
        System.out.println();
    }

    /**
     * Flattens the Netflix JSON, applies {@link UpperCaseMapFunction} with the two-wildcard
     * pattern {@code "search_engines.*.list.*"}, then prints the Row grouped by engine.
     *
     * <p>The pattern matches {@code search_engines.0.list.0}, {@code search_engines.0.list.1},
     * {@code search_engines.1.list.0}, … across all engines and all list positions.
     * Every URL field is uppercased; all other fields are unchanged.
     */
    @Test
    void printNetflixFlatRow_allListUrlsUppercased() throws Exception {
        // Step 1: flatten
        List<Row> rows = flattenBytes(NetflixCategoriesTest.NETFLIX_BYTES);
        assertEquals(1, rows.size());
        Row flatRow = rows.get(0);

        // Step 2: uppercase every list URL across all engines
        Row out = applyUpperCase(List.of("search_engines.*.list.*"), flatRow);

        Set<String> fieldNames = out.getFieldNames(false);
        assertNotNull(fieldNames);

        System.out.println();
        System.out.println("=== Netflix flat Row — all list URLs uppercased ===");
        System.out.println("    pattern: \"search_engines.*.list.*\"");
        System.out.println();

        // Application section (unchanged)
        System.out.println("── application ──────────────────────────────────");
        fieldNames.stream()
                .filter(n -> n.startsWith("application."))
                .sorted()
                .forEach(n -> System.out.printf("  %-45s = %s%n", n, out.getField(n)));

        // Per-engine sections
        for (int i = 0; i < 5; i++) {
            String prefix = "search_engines." + i + ".";
            boolean hasAny = fieldNames.stream().anyMatch(n -> n.startsWith(prefix));
            if (!hasAny) break;

            System.out.println();
            System.out.println("── search_engines[" + i + "] ──────────────────────────────");
            fieldNames.stream()
                    .filter(n -> n.startsWith(prefix))
                    .sorted()
                    .forEach(n -> {
                        Object value = out.getField(n);
                        boolean changed = n.matches("search_engines\\.\\d+\\.list\\.\\d+");
                        System.out.printf("  %-45s = %-55s%s%n",
                                n,
                                value,
                                changed ? "  ◄ uppercased" : "");
                    });
        }
        System.out.println();
    }
}
