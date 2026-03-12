package com.pipeline.processing;

import com.pipeline.config.PipelineConfig;
import com.pipeline.common.ProcessedMessage;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;

/**
 * Flink {@link RichMapFunction} that applies a third-party string processor to
 * a named field in each Flink {@link Row} (named-field mode), then returns
 * the mutated row.
 *
 * <h2>Named field access on Flink's Row</h2>
 * Fields are accessed by name using the Row's named API:
 * <ul>
 *   <li>{@link Row#getFieldNames(boolean)} — obtain the current set of field names</li>
 *   <li>{@link Row#getField(String)} — read a field value by dot-notation key</li>
 *   <li>{@link Row#setField(String, Object)} — write a field value by dot-notation key</li>
 * </ul>
 *
 * <h2>Field key resolution</h2>
 * The configured {@code uppercaseFieldKeys} list is tried in priority order.
 * The first key found in the Row is processed; subsequent keys are skipped.
 * This allows the same function to handle both flat ({@code name}) and nested
 * ({@code person.name}) JSON schemas without branching in pipeline configuration.
 *
 * <h2>Third-party JAR loading</h2>
 * Loaded once per TaskManager subtask in {@link #open(Configuration)} via
 * {@link URLClassLoader}. A bound {@link MethodHandle} is resolved from the
 * vendor class's {@code String process(String)} method — roughly 10× faster
 * than {@code Method.invoke()} after JIT warm-up.
 *
 * <h2>Fallback</h2>
 * If {@code processing.third-party-jar} is blank, the function falls back to
 * {@link String#toUpperCase()} for development / integration testing without
 * the vendor JAR.
 */
public final class UpperCaseMapFunction extends RichMapFunction<ProcessedMessage, ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UpperCaseMapFunction.class);

    // ── Serializable config ───────────────────────────────────────────────────

    private final String       jarPath;
    private final String       processorClassName;
    private final List<String> fieldKeys;

    // ── Transient state: re-initialised in open(), NOT serialised ─────────────

    /** Bound MethodHandle: {@code invoke(String) → String}. Null in fallback mode. */
    private transient MethodHandle  processHandle;
    private transient URLClassLoader classLoader;

    private transient Counter processedCount;
    private transient Counter skippedCount;

    // ── Constructor ───────────────────────────────────────────────────────────

    public UpperCaseMapFunction(PipelineConfig config) {
        this.jarPath            = config.getThirdPartyJarPath();
        this.processorClassName = config.getProcessorClassName();
        this.fieldKeys          = config.getUppercaseFieldKeys();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) throws Exception {
        processedCount = getRuntimeContext().getMetricGroup().counter("row.field.processed");
        skippedCount   = getRuntimeContext().getMetricGroup().counter("row.field.no_match");

        if (jarPath != null && !jarPath.isBlank()) {
            loadVendorJar();
        } else {
            LOG.warn("processing.third-party-jar not configured; "
                    + "using String.toUpperCase() fallback");
        }

        LOG.info("UpperCaseMapFunction subtask {}/{}: jar='{}', class='{}', keys={}",
                getRuntimeContext().getIndexOfThisSubtask() + 1,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                jarPath, processorClassName, fieldKeys);
    }

    private void loadVendorJar() throws Exception {
        File jarFile = new File(jarPath);
        if (!jarFile.exists() || !jarFile.isFile()) {
            throw new IllegalStateException("Third-party JAR not found: " + jarPath);
        }

        classLoader = new URLClassLoader(
                new URL[]{jarFile.toURI().toURL()},
                getClass().getClassLoader());

        Class<?>   clazz     = classLoader.loadClass(processorClassName);
        Object     instance  = clazz.getDeclaredConstructor().newInstance();
        MethodHandle unbound = MethodHandles.lookup()
                .findVirtual(clazz, "process", MethodType.methodType(String.class, String.class));

        processHandle = unbound.bindTo(instance);
        LOG.info("Vendor processor loaded: {}", processorClassName);
    }

    @Override
    public void close() throws Exception {
        if (classLoader != null) {
            classLoader.close();
            classLoader = null;
        }
    }

    // ── Hot path ──────────────────────────────────────────────────────────────

    /**
     * For every field name in the row, applies the processor to the value if any
     * configured pattern matches it.  All matching fields are processed — not just the
     * first — enabling bulk operations such as uppercasing every director across all
     * array elements with a single wildcard pattern.
     *
     * <p>Pattern evaluation per field: patterns in {@code fieldKeys} are tried in
     * order; the first pattern that matches a given field name wins (subsequent
     * patterns are skipped for that field).
     */
    @Override
    public ProcessedMessage map(ProcessedMessage msg) throws Exception {
        Row row = msg.getPayload();
        if (row == null) return msg;

        // getFieldNames(false) returns the LinkedHashSet of names for a named Row;
        // it is null only for positional rows — never null here.
        Set<String> names = row.getFieldNames(false);
        if (names == null) return msg;

        boolean anyProcessed = false;
        for (String fieldName : names) {
            for (String pattern : fieldKeys) {
                if (!matches(pattern, fieldName)) continue;

                Object raw = row.getField(fieldName);
                if (raw instanceof String) {
                    row.setField(fieldName, applyProcessor((String) raw));
                    processedCount.inc();
                    anyProcessed = true;
                }
                break; // first matching pattern wins for this field name
            }
        }

        if (!anyProcessed) skippedCount.inc();
        return msg.withPayload(row);
    }

    /**
     * Returns {@code true} when {@code pattern} matches {@code fieldName}.
     *
     * <p>An asterisk {@code *} standing alone as a dot-separated segment matches
     * exactly one segment of the field name (including numeric array indices such as
     * {@code "0"} or {@code "42"}).  A pattern without any {@code *} is an exact,
     * case-sensitive match.
     *
     * <p>Examples:
     * <pre>
     *   matches("person.name",                    "person.name")             → true
     *   matches("search_engines.*.imdb.director", "search_engines.0.imdb.director") → true
     *   matches("search_engines.*.imdb.director", "search_engines.4.imdb.director") → true
     *   matches("search_engines.*.imdb.director", "search_engines.0.imdb.title")    → false
     *   matches("search_engines.*.imdb.director", "application.name")               → false
     * </pre>
     */
    static boolean matches(String pattern, String fieldName) {
        // Fast path: exact match
        if (pattern.equals(fieldName)) return true;
        // Fast path: no wildcard — only exact would match (already checked)
        if (!pattern.contains("*")) return false;

        String[] patParts  = pattern.split("\\.", -1);
        String[] nameParts = fieldName.split("\\.", -1);
        if (patParts.length != nameParts.length) return false;

        for (int i = 0; i < patParts.length; i++) {
            if (!"*".equals(patParts[i]) && !patParts[i].equals(nameParts[i])) {
                return false;
            }
        }
        return true;
    }

    private String applyProcessor(String input) throws Exception {
        if (processHandle != null) {
            try {
                return (String) processHandle.invoke(input);
            } catch (Throwable t) {
                throw new RuntimeException("Vendor processor threw for input [" + input + "]", t);
            }
        }
        return input.toUpperCase(java.util.Locale.ROOT);
    }
}
