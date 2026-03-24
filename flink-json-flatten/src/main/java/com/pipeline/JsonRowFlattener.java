package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.types.Row;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Shared JSON validation and flattening helpers used by the JSON input stage. */
final class JsonRowFlattener {

    private JsonRowFlattener() {}

    static List<String> validate(JsonNode root, InputSchemaInfo inputSchema) {
        List<String> violations = new ArrayList<>();
        for (String path : inputSchema.getRequiredFields()) {
            collectMissingRequiredPaths(root, path, "", violations);
        }
        validateFieldTypes(root, inputSchema.getFieldTypes(), "", violations);
        return new ArrayList<>(new LinkedHashSet<>(violations));
    }

    static boolean isMissingRequired(JsonNode node, String dotPath) {
        List<String> missing = new ArrayList<>(1);
        collectMissingRequiredPaths(node, dotPath, "", missing);
        return !missing.isEmpty();
    }

    private static void collectMissingRequiredPaths(JsonNode node,
                                                    String dotPath,
                                                    String pathPrefix,
                                                    List<String> missingPaths) {
        collectMissingRequiredPaths(node, dotPath.split("\\."), 0, pathPrefix, missingPaths);
    }

    private static void collectMissingRequiredPaths(JsonNode node,
                                                    String[] parts,
                                                    int index,
                                                    String currentPath,
                                                    List<String> missingPaths) {
        if (index >= parts.length || node == null || node.isMissingNode()) {
            return;
        }

        if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String arrayPath = currentPath.isEmpty() ? String.valueOf(i) : currentPath + "." + i;
                collectMissingRequiredPaths(node.get(i), parts, index, arrayPath, missingPaths);
            }
            return;
        }

        String part = parts[index];
        JsonNode child = node.path(part);
        String nextPath = currentPath.isEmpty() ? part : currentPath + "." + part;

        if (index == parts.length - 1) {
            if (child.isMissingNode()) {
                missingPaths.add("missing required field '" + nextPath + "'");
            }
            return;
        }

        if (child.isMissingNode()) {
            return;
        }

        collectMissingRequiredPaths(child, parts, index + 1, nextPath, missingPaths);
    }

    private static void validateFieldTypes(JsonNode node,
                                           Map<String, String> fieldTypes,
                                           String pathPrefix,
                                           List<String> violations) {
        fieldTypes.forEach((path, expectedType) ->
                collectTypeViolations(node, path.split("\\."), 0, pathPrefix, expectedType, violations));
    }

    private static void collectTypeViolations(JsonNode node,
                                              String[] parts,
                                              int index,
                                              String currentPath,
                                              String expectedType,
                                              List<String> violations) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return;
        }

        if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String arrayPath = currentPath.isEmpty() ? String.valueOf(i) : currentPath + "." + i;
                collectTypeViolations(node.get(i), parts, index, arrayPath, expectedType, violations);
            }
            return;
        }

        if (index >= parts.length) {
            return;
        }

        String part = parts[index];
        JsonNode child = node.path(part);
        String nextPath = currentPath.isEmpty() ? part : currentPath + "." + part;
        if (child.isMissingNode() || child.isNull()) {
            return;
        }

        if (index == parts.length - 1) {
            if (!matchesType(child, expectedType)) {
                violations.add("incorrect type for field '" + nextPath + "': expected "
                        + expectedType + " but was " + actualType(child));
            }
            return;
        }

        collectTypeViolations(child, parts, index + 1, nextPath, expectedType, violations);
    }

    private static boolean matchesType(JsonNode value, String expectedType) {
        switch (expectedType) {
            case "integer":
                return value.isIntegralNumber();
            case "number":
                return value.isNumber();
            case "string":
                return value.isTextual();
            case "boolean":
                return value.isBoolean();
            case "object":
                return value.isObject();
            case "array":
                return value.isArray();
            default:
                return true;
        }
    }

    private static String actualType(JsonNode value) {
        if (value.isIntegralNumber()) return "integer";
        if (value.isFloatingPointNumber()) return "number";
        if (value.isTextual()) return "string";
        if (value.isBoolean()) return "boolean";
        if (value.isArray()) return "array";
        if (value.isObject()) return "object";
        if (value.isNull()) return "null";
        return value.getNodeType().name().toLowerCase();
    }

    static void flattenInto(JsonNode node, String prefix, Row row, NullHandling nullHandling) {
        Deque<Object[]> stack = new ArrayDeque<>(32);
        stack.push(new Object[]{prefix, node});

        while (!stack.isEmpty()) {
            Object[] frame = stack.pop();
            String p = (String) frame[0];
            JsonNode n = (JsonNode) frame[1];

            if (n.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = n.fields();
                List<Object[]> children = new ArrayList<>();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> e = fields.next();
                    String childKey = p.isEmpty() ? e.getKey() : p + "." + e.getKey();
                    children.add(new Object[]{childKey, e.getValue()});
                }
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }
            } else if (n.isArray()) {
                for (int i = n.size() - 1; i >= 0; i--) {
                    String childKey = p.isEmpty() ? String.valueOf(i) : p + "." + i;
                    stack.push(new Object[]{childKey, n.get(i)});
                }
            } else {
                Object value = extractLeafValue(n);
                if (value == null) {
                    if (nullHandling == NullHandling.EXCLUDE) continue;
                    if (nullHandling == NullHandling.REPLACE_EMPTY_STRING) value = "";
                }
                row.setField(p, value);
            }
        }
    }

    static Object extractLeafValue(JsonNode node) {
        if (node.isNull() || node.isMissingNode()) return null;
        if (node.isIntegralNumber()) {
            long lv = node.longValue();
            return (lv >= Integer.MIN_VALUE && lv <= Integer.MAX_VALUE) ? (int) lv : lv;
        }
        if (node.isFloatingPointNumber()) {
            return node.isBigDecimal() ? node.decimalValue() : node.doubleValue();
        }
        if (node.isBoolean()) return node.booleanValue();
        return node.asText();
    }
}
