package com.pipeline.splitting.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.PaginationSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pure-static utility that derives a {@link PaginationSchema} from any JSON Schema document.
 *
 * <h2>Schema compatibility</h2>
 * Works with any structurally valid JSON Schema regardless of how properties are declared:
 * <ul>
 *   <li>Flat {@code "properties"} object at the root</li>
 *   <li>Properties split across {@code allOf} / {@code anyOf} / {@code oneOf} sub-schemas</li>
 *   <li>Properties inside {@code if} / {@code then} / {@code else} branches</li>
 *   <li>Properties defined in {@code $defs} / {@code definitions} and referenced inline
 *       (resolved by name when the referencing sub-schema is a simple {@code "$ref"})</li>
 *   <li>Any combination of the above, arbitrarily nested</li>
 *   <li>No {@code "required"} array needed — all discovered properties are candidates</li>
 * </ul>
 *
 * <h2>Detection heuristics</h2>
 * <table border="1">
 *   <tr><th>Role</th><th>Rule</th></tr>
 *   <tr><td>array data</td><td>property with {@code "type":"array"}</td></tr>
 *   <tr><td>total pages</td><td>only integer property with {@code "minimum":1}</td></tr>
 *   <tr><td>page index</td><td>integer with {@code "minimum":0} and description
 *       containing "index", "current", or "0-based" (case-insensitive)</td></tr>
 *   <tr><td>item count</td><td>the sole remaining integer property</td></tr>
 * </table>
 *
 * <p>Throws {@link IllegalArgumentException} if any role cannot be uniquely identified.
 */
public final class SchemaAnalyzer {

    private SchemaAnalyzer() {}

    /** JSON Schema composition keywords whose sub-schemas are traversed. */
    private static final String[] COMPOSITION_KEYS = {"allOf", "anyOf", "oneOf"};

    /** JSON Schema conditional keywords whose sub-schemas are traversed. */
    private static final String[] CONDITIONAL_KEYS = {"if", "then", "else"};

    /** JSON Schema definition-container keywords (used for local $ref resolution). */
    private static final String[] DEFINITION_KEYS = {"$defs", "definitions"};

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Loads a JSON Schema from the classpath.
     *
     * @param classpathPath resource path, e.g. {@code "schemas/output-persons-paginated.schema.json"}
     * @return parsed {@link JsonNode} root
     * @throws IOException           if the resource cannot be read
     * @throws IllegalStateException if the resource is not found on the classpath
     */
    public static JsonNode loadSchema(String classpathPath) throws IOException {
        try (InputStream is = SchemaAnalyzer.class.getClassLoader()
                .getResourceAsStream(classpathPath)) {
            if (is == null) {
                throw new IllegalStateException("Schema not found on classpath: " + classpathPath);
            }
            return new ObjectMapper().readTree(is);
        }
    }

    /**
     * Extracts the ordered list of CSV column names from a JSON Schema.
     *
     * <h2>Contract</h2>
     * <ul>
     *   <li>The schema <em>must</em> have a non-empty {@code "required"} array.
     *       Column order in the CSV matches the order of entries in that array.</li>
     *   <li>If a {@code "properties"} object is present, every key in it must also
     *       appear in {@code "required"} — there are no optional CSV columns.</li>
     *   <li>Column names may use dot-notation to reference flat
     *       {@link org.apache.flink.types.Row} fields (e.g. {@code "address.street"}).</li>
     * </ul>
     *
     * @param schemaRoot root {@link JsonNode} of the JSON Schema document
     * @return immutable, ordered list of column names
     * @throws IllegalArgumentException if the schema violates the CSV contract
     */
    public static List<String> extractCsvColumns(JsonNode schemaRoot) {
        JsonNode required = schemaRoot.path("required");
        if (!required.isArray() || required.isEmpty()) {
            throw new IllegalArgumentException(
                    "CSV schema must have a non-empty 'required' array defining column order");
        }

        List<String> columns = new ArrayList<>(required.size());
        for (JsonNode col : required) {
            String name = col.asText();
            if (name.isBlank()) {
                throw new IllegalArgumentException(
                        "CSV schema 'required' array contains a blank column name");
            }
            columns.add(name);
        }

        // Validate: every property must be required (no optional columns in CSV)
        JsonNode properties = schemaRoot.path("properties");
        if (properties.isObject()) {
            Set<String> requiredSet = new HashSet<>(columns);
            Iterator<String> propNames = properties.fieldNames();
            while (propNames.hasNext()) {
                String prop = propNames.next();
                if (!requiredSet.contains(prop)) {
                    throw new IllegalArgumentException(
                            "CSV schema property '" + prop + "' is not listed in 'required'. "
                            + "All CSV columns must be required.");
                }
            }
        }

        return Collections.unmodifiableList(columns);
    }

    /**
     * Analyzes the supplied JSON Schema root and returns the extracted {@link PaginationSchema}.
     *
     * @param schemaRoot root {@link JsonNode} of the JSON Schema document
     * @return pagination field-name mapping
     * @throws IllegalArgumentException if any role cannot be uniquely identified
     */
    public static PaginationSchema analyze(JsonNode schemaRoot) {
        Map<String, JsonNode> props = new LinkedHashMap<>();
        collectFlatProperties(schemaRoot, schemaRoot, "", props);

        if (props.isEmpty()) {
            throw new IllegalArgumentException(
                    "No properties found anywhere in schema "
                    + "(searched properties, allOf/anyOf/oneOf, if/then/else, $defs/definitions)");
        }

        // ── Role detection ────────────────────────────────────────────────────

        String arrayField = findArrayField(props);

        List<String> intFields = props.entrySet().stream()
                .filter(e -> !e.getKey().equals(arrayField))
                .filter(e -> "integer".equals(e.getValue().path("type").asText()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        String totalField = findTotalField(props, intFields);

        List<String> indexCandidates = intFields.stream()
                .filter(f -> !f.equals(totalField))
                .collect(Collectors.toList());
        String indexField = findIndexField(props, indexCandidates);

        List<String> remaining = intFields.stream()
                .filter(f -> !f.equals(totalField) && !f.equals(indexField))
                .collect(Collectors.toList());
        String countField = findCountField(remaining);

        return new PaginationSchema(arrayField, indexField, totalField, countField);
    }

    // ── Property collection ───────────────────────────────────────────────────

    /**
     * Like {@link #collectProperties} but also recurses into {@code object}-typed property
     * values, emitting their sub-properties with dot-notation keys (e.g. a {@code metadata}
     * object containing {@code page} is emitted as {@code "metadata.page"}).
     *
     * <p>This allows {@link #analyze} to detect pagination fields that are nested inside
     * a wrapper object (e.g. {@code metadata.page}, {@code metadata.totalCount}).
     *
     * <p>Array-typed properties and leaf properties are emitted at their current prefix,
     * not descended into.
     */
    static void collectFlatProperties(JsonNode node, JsonNode schemaRoot,
                                      String prefix, Map<String, JsonNode> result) {
        if (node == null || node.isMissingNode() || !node.isObject()) return;

        // Follow a local $ref
        JsonNode ref = node.path("$ref");
        if (ref.isTextual()) {
            JsonNode resolved = resolveRef(ref.asText(), schemaRoot);
            if (resolved != null) collectFlatProperties(resolved, schemaRoot, prefix, result);
            return;
        }

        // Direct "properties" object
        JsonNode properties = node.path("properties");
        if (properties.isObject()) {
            properties.fields().forEachRemaining(e -> {
                String key   = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
                JsonNode val = e.getValue();
                if ("object".equals(val.path("type").asText())) {
                    // Recurse into nested object — emit its sub-properties with dot-notation
                    collectFlatProperties(val, schemaRoot, key, result);
                } else {
                    result.putIfAbsent(key, val);
                }
            });
        }

        // Composition keywords
        for (String k : COMPOSITION_KEYS) {
            JsonNode arr = node.path(k);
            if (arr.isArray()) arr.forEach(sub -> collectFlatProperties(sub, schemaRoot, prefix, result));
        }

        // Conditional keywords
        for (String k : CONDITIONAL_KEYS) {
            JsonNode sub = node.path(k);
            if (!sub.isMissingNode()) collectFlatProperties(sub, schemaRoot, prefix, result);
        }

        // Definition containers
        for (String k : DEFINITION_KEYS) {
            JsonNode defs = node.path(k);
            if (defs.isObject()) defs.forEach(def -> collectFlatProperties(def, schemaRoot, prefix, result));
        }
    }

    /**
     * Recursively collects property definitions from {@code node} into {@code result},
     * traversing composition ({@code allOf/anyOf/oneOf}), conditional ({@code if/then/else}),
     * and definition-container ({@code $defs/definitions}) keywords.
     *
     * <p>Simple {@code $ref} values of the form {@code "#/$defs/Foo"} or
     * {@code "#/definitions/Foo"} are resolved against {@code schemaRoot}.
     *
     * <p>Does <em>not</em> recurse into property values (e.g. an array property's
     * {@code "items"} sub-schema), so only sibling-level pagination fields are collected.
     *
     * <p>First definition wins when the same name appears in multiple sub-schemas.
     */
    public static void collectProperties(JsonNode node, JsonNode schemaRoot,
                                  Map<String, JsonNode> result) {
        if (node == null || node.isMissingNode() || !node.isObject()) return;

        // Follow a local $ref before doing anything else
        JsonNode ref = node.path("$ref");
        if (ref.isTextual()) {
            JsonNode resolved = resolveRef(ref.asText(), schemaRoot);
            if (resolved != null) {
                collectProperties(resolved, schemaRoot, result);
            }
            // A $ref node typically has no siblings worth traversing — stop here
            return;
        }

        // Direct "properties" object
        JsonNode properties = node.path("properties");
        if (properties.isObject()) {
            properties.fields().forEachRemaining(e ->
                    result.putIfAbsent(e.getKey(), e.getValue()));
        }

        // Composition keywords: each element is a sub-schema
        for (String key : COMPOSITION_KEYS) {
            JsonNode arr = node.path(key);
            if (arr.isArray()) {
                arr.forEach(sub -> collectProperties(sub, schemaRoot, result));
            }
        }

        // Conditional keywords: single sub-schema each
        for (String key : CONDITIONAL_KEYS) {
            JsonNode sub = node.path(key);
            if (!sub.isMissingNode()) {
                collectProperties(sub, schemaRoot, result);
            }
        }

        // Definition containers: traverse each named definition
        for (String key : DEFINITION_KEYS) {
            JsonNode defs = node.path(key);
            if (defs.isObject()) {
                defs.forEach(def -> collectProperties(def, schemaRoot, result));
            }
        }
    }

    /**
     * Resolves a JSON Pointer {@code $ref} string against the schema root.
     * Handles {@code "#/$defs/Foo"} and {@code "#/definitions/Foo"}.
     * Returns {@code null} for any ref that cannot be resolved locally.
     */
    private static JsonNode resolveRef(String ref, JsonNode schemaRoot) {
        if (!ref.startsWith("#/")) return null;
        String[] parts = ref.substring(2).split("/");
        JsonNode current = schemaRoot;
        for (String part : parts) {
            // Unescape JSON Pointer tokens (~1 = '/', ~0 = '~')
            part = part.replace("~1", "/").replace("~0", "~");
            current = current.path(part);
            if (current.isMissingNode()) return null;
        }
        return current;
    }

    // ── Role detection (heuristics) ───────────────────────────────────────────

    /**
     * Returns the property whose {@code "type"} is {@code "array"}.
     * Throws if none or multiple are found.
     */
    private static String findArrayField(Map<String, JsonNode> props) {
        List<String> matches = props.entrySet().stream()
                .filter(e -> "array".equals(e.getValue().path("type").asText()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (matches.size() != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one array-typed property, found: " + matches);
        }
        return matches.get(0);
    }

    /**
     * Returns the integer field whose {@code "minimum"} is exactly {@code 1}.
     * Uniquely identifies the total-pages field (always ≥ 1).
     * Throws if none or multiple match.
     */
    private static String findTotalField(Map<String, JsonNode> props, List<String> intFields) {
        List<String> matches = intFields.stream()
                .filter(f -> {
                    JsonNode min = props.get(f).path("minimum");
                    return !min.isMissingNode() && min.asInt() == 1;
                })
                .collect(Collectors.toList());

        if (matches.size() != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one integer field with minimum:1 (total-pages), found: " + matches);
        }
        return matches.get(0);
    }

    /**
     * Returns the integer field whose {@code "minimum"} is {@code 0} AND whose
     * {@code "description"} contains "index", "current", or "0-based" (case-insensitive).
     * Throws if none or multiple match.
     */
    private static String findIndexField(Map<String, JsonNode> props, List<String> candidates) {
        List<String> matches = candidates.stream()
                .filter(f -> {
                    JsonNode fieldNode = props.get(f);
                    JsonNode min = fieldNode.path("minimum");
                    if (min.isMissingNode() || min.asInt() != 0) return false;
                    String desc = fieldNode.path("description").asText("").toLowerCase();
                    return desc.contains("index") || desc.contains("current") || desc.contains("0-based");
                })
                .collect(Collectors.toList());

        if (matches.size() != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one index field (minimum:0 + description containing "
                    + "\"index\"/\"current\"/\"0-based\"), found: " + matches);
        }
        return matches.get(0);
    }

    /**
     * Returns the sole remaining integer field — the item-count field.
     * Throws if the list does not contain exactly one element.
     */
    private static String findCountField(List<String> remaining) {
        if (remaining.size() != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one remaining integer field (item-count), found: " + remaining);
        }
        return remaining.get(0);
    }
}
