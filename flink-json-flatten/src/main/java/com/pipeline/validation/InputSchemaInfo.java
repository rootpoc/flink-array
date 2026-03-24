package com.pipeline.validation;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Immutable value object holding the validation rules extracted from an input JSON Schema.
 *
 * <p>Use {@link #analyze(JsonNode)} to build an instance from a JSON Schema document.
 *
 * <h2>Extracted information</h2>
 * <ul>
 *   <li><b>Array field name</b> — the first {@code "type":"array"} property at the root level.</li>
 *   <li><b>Required fields</b> — fields required in all valid documents at any depth.</li>
 *   <li><b>Required item fields</b> — the same extraction applied to the array's {@code items} sub-schema.</li>
 *   <li><b>Conditional required fields</b> — fields required by one or more {@code anyOf}/{@code oneOf}
 *       branches, exposed for schema introspection but not used as unconditional validation rules.</li>
 * </ul>
 */
public final class InputSchemaInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String       arrayFieldName;
    private final List<String> requiredFields;
    private final List<String> requiredItemFields;
    private final List<String> conditionalRequiredFields;
    private final List<String> conditionalRequiredItemFields;
    private final Map<String, String> fieldTypes;
    private final Map<String, String> itemFieldTypes;

    public InputSchemaInfo(
            String arrayFieldName,
            List<String> requiredFields,
            List<String> requiredItemFields,
            List<String> conditionalRequiredFields,
            List<String> conditionalRequiredItemFields,
            Map<String, String> fieldTypes,
            Map<String, String> itemFieldTypes) {
        this.arrayFieldName = arrayFieldName;
        this.requiredFields = List.copyOf(requiredFields);
        this.requiredItemFields = List.copyOf(requiredItemFields);
        this.conditionalRequiredFields = List.copyOf(conditionalRequiredFields);
        this.conditionalRequiredItemFields = List.copyOf(conditionalRequiredItemFields);
        this.fieldTypes = Map.copyOf(fieldTypes);
        this.itemFieldTypes = Map.copyOf(itemFieldTypes);
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    /**
     * Analyzes the supplied JSON Schema root and returns the extracted {@link InputSchemaInfo}.
     *
     * @param schema root {@link JsonNode} of the input JSON Schema document
     * @return validation rules derived from the schema
     */
    public static InputSchemaInfo analyze(JsonNode schema) {
        String arrayFieldName = findArrayField(schema);
        List<String> required = extractRequired(schema, schema);
        List<String> itemRequired = extractItemRequired(schema, arrayFieldName);
        List<String> conditionalRequired = extractConditionalRequired(schema, schema);
        List<String> conditionalItemRequired = extractConditionalItemRequired(schema, arrayFieldName);
        Map<String, String> fieldTypes = extractLeafTypes(schema, schema);
        Map<String, String> itemFieldTypes = extractItemLeafTypes(schema, arrayFieldName);
        return new InputSchemaInfo(
                arrayFieldName,
                required,
                itemRequired,
                conditionalRequired,
                conditionalItemRequired,
                fieldTypes,
                itemFieldTypes);
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /** Name of the array-typed root property, or {@code null} for flat schemas. */
    public String       getArrayFieldName()     { return arrayFieldName; }
    public List<String> getRequiredFields()     { return requiredFields; }
    public List<String> getRequiredItemFields() { return requiredItemFields; }
    public List<String> getConditionalRequiredFields() { return conditionalRequiredFields; }
    public List<String> getConditionalRequiredItemFields() { return conditionalRequiredItemFields; }
    public Map<String, String> getFieldTypes() { return fieldTypes; }
    public Map<String, String> getItemFieldTypes() { return itemFieldTypes; }

    @Override
    public String toString() {
        return "InputSchemaInfo{arrayField='" + arrayFieldName
                + "', required=" + requiredFields
                + ", requiredItems=" + requiredItemFields
                + ", conditionalRequired=" + conditionalRequiredFields
                + ", conditionalRequiredItems=" + conditionalRequiredItemFields
                + ", fieldTypes=" + fieldTypes
                + ", itemFieldTypes=" + itemFieldTypes + '}';
    }

    // ── Schema analysis (private) ─────────────────────────────────────────────

    private static String findArrayField(JsonNode schema) {
        JsonNode props = schema.path("properties");
        if (props.isObject()) {
            var it = props.fields();
            while (it.hasNext()) {
                var entry = it.next();
                if ("array".equals(entry.getValue().path("type").asText())) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    private static List<String> extractRequired(JsonNode schema, JsonNode schemaRoot) {
        Set<String> result = new LinkedHashSet<>();
        collectRequiredPaths(schema, schemaRoot, "", result);
        return new ArrayList<>(result);
    }

    private static List<String> extractConditionalRequired(JsonNode schema, JsonNode schemaRoot) {
        Set<String> result = new LinkedHashSet<>();
        collectConditionalRequiredPaths(schema, schemaRoot, "", result);
        return new ArrayList<>(result);
    }

    private static void collectRequiredPaths(JsonNode schema, JsonNode schemaRoot, String prefix, Set<String> result) {
        JsonNode resolved = resolveLocalRef(schema, schemaRoot);
        if (resolved != null) {
            collectRequiredPaths(resolved, schemaRoot, prefix, result);
            return;
        }

        JsonNode req = schema.path("required");
        if (req.isArray()) {
            req.forEach(n -> result.add(prefix + n.asText()));
        }

        JsonNode props = schema.path("properties");
        if (props.isObject()) {
            props.fields().forEachRemaining(entry -> {
                JsonNode propSchema = entry.getValue();
                if (shouldTraverseSchema(propSchema)) {
                    collectRequiredPaths(propSchema, schemaRoot, prefix + entry.getKey() + ".", result);
                }
            });
        }

        JsonNode items = schema.path("items");
        if (!items.isMissingNode()) {
            collectRequiredPaths(items, schemaRoot, prefix, result);
        }

        JsonNode allOf = schema.path("allOf");
        if (allOf.isArray()) {
            allOf.forEach(sub -> collectRequiredPaths(sub, schemaRoot, prefix, result));
        }

        for (String key : new String[]{"anyOf", "oneOf"}) {
            JsonNode composition = schema.path(key);
            if (composition.isArray() && !composition.isEmpty()) {
                result.addAll(intersectRequired(composition, schemaRoot, prefix));
            }
        }
    }

    private static void collectConditionalRequiredPaths(JsonNode schema, JsonNode schemaRoot, String prefix, Set<String> result) {
        JsonNode resolved = resolveLocalRef(schema, schemaRoot);
        if (resolved != null) {
            collectConditionalRequiredPaths(resolved, schemaRoot, prefix, result);
            return;
        }

        JsonNode props = schema.path("properties");
        if (props.isObject()) {
            props.fields().forEachRemaining(entry -> {
                JsonNode propSchema = entry.getValue();
                if (shouldTraverseSchema(propSchema)) {
                    collectConditionalRequiredPaths(propSchema, schemaRoot, prefix + entry.getKey() + ".", result);
                }
            });
        }

        JsonNode items = schema.path("items");
        if (!items.isMissingNode()) {
            collectConditionalRequiredPaths(items, schemaRoot, prefix, result);
        }

        JsonNode allOf = schema.path("allOf");
        if (allOf.isArray()) {
            allOf.forEach(sub -> collectConditionalRequiredPaths(sub, schemaRoot, prefix, result));
        }

        for (String key : new String[]{"anyOf", "oneOf"}) {
            JsonNode composition = schema.path(key);
            if (composition.isArray()) {
                composition.forEach(branch -> {
                    Set<String> branchRequired = new LinkedHashSet<>();
                    collectRequiredPaths(branch, schemaRoot, prefix, branchRequired);
                    result.addAll(branchRequired);
                    collectConditionalRequiredPaths(branch, schemaRoot, prefix, result);
                });
            }
        }
    }

    private static Set<String> intersectRequired(JsonNode branches, JsonNode schemaRoot, String prefix) {
        Set<String> intersection = null;
        for (JsonNode branch : branches) {
            Set<String> branchRequired = new LinkedHashSet<>();
            collectRequiredPaths(branch, schemaRoot, prefix, branchRequired);
            if (intersection == null) {
                intersection = new LinkedHashSet<>(branchRequired);
            } else {
                intersection.retainAll(branchRequired);
            }
        }
        return intersection != null ? intersection : Set.of();
    }

    private static List<String> extractItemRequired(JsonNode schema, String arrayFieldName) {
        if (arrayFieldName == null) return List.of();
        JsonNode items = schema.path("properties").path(arrayFieldName).path("items");
        return extractRequired(items, schema);
    }

    private static List<String> extractConditionalItemRequired(JsonNode schema, String arrayFieldName) {
        if (arrayFieldName == null) return List.of();
        JsonNode items = schema.path("properties").path(arrayFieldName).path("items");
        return extractConditionalRequired(items, schema);
    }

    private static Map<String, String> extractLeafTypes(JsonNode schema, JsonNode schemaRoot) {
        Map<String, String> result = new LinkedHashMap<>();
        collectLeafTypes(schema, schemaRoot, "", result);
        return result;
    }

    private static Map<String, String> extractItemLeafTypes(JsonNode schema, String arrayFieldName) {
        if (arrayFieldName == null) return Map.of();
        JsonNode items = schema.path("properties").path(arrayFieldName).path("items");
        return extractLeafTypes(items, schema);
    }

    private static void collectLeafTypes(JsonNode schema, JsonNode schemaRoot, String prefix, Map<String, String> result) {
        JsonNode resolved = resolveLocalRef(schema, schemaRoot);
        if (resolved != null) {
            collectLeafTypes(resolved, schemaRoot, prefix, result);
            return;
        }

        JsonNode props = schema.path("properties");
        if (props.isObject()) {
            props.fields().forEachRemaining(entry -> {
                JsonNode propSchema = entry.getValue();
                String path = prefix + entry.getKey();
                String type = propSchema.path("type").asText();
                if (!type.isBlank()) {
                    result.putIfAbsent(path, type);
                }
                if (shouldTraverseSchema(propSchema)) {
                    collectLeafTypes(propSchema, schemaRoot, path + ".", result);
                }
            });
        }

        JsonNode items = schema.path("items");
        if (!items.isMissingNode()) {
            collectLeafTypes(items, schemaRoot, prefix, result);
        }

        JsonNode allOf = schema.path("allOf");
        if (allOf.isArray()) {
            allOf.forEach(sub -> collectLeafTypes(sub, schemaRoot, prefix, result));
        }

        for (String key : new String[]{"anyOf", "oneOf"}) {
            JsonNode composition = schema.path(key);
            if (composition.isArray()) {
                composition.forEach(sub -> collectLeafTypes(sub, schemaRoot, prefix, result));
            }
        }
    }

    private static boolean shouldTraverseSchema(JsonNode schema) {
        return "object".equals(schema.path("type").asText())
                || "array".equals(schema.path("type").asText())
                || schema.has("properties")
                || schema.has("items")
                || schema.has("$ref")
                || schema.has("allOf")
                || schema.has("anyOf")
                || schema.has("oneOf");
    }

    private static JsonNode resolveLocalRef(JsonNode schema, JsonNode schemaRoot) {
        JsonNode ref = schema.path("$ref");
        if (!ref.isTextual()) return null;
        String refText = ref.asText();
        if (!refText.startsWith("#/")) return null;

        JsonNode current = schemaRoot;
        for (String part : refText.substring(2).split("/")) {
            part = part.replace("~1", "/").replace("~0", "~");
            current = current.path(part);
            if (current.isMissingNode()) return null;
        }
        return current;
    }
}
