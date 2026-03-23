package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.pipeline.config.PipelineConfig.NullHandling;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.types.Row;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Shared JSON validation and flattening helpers used by the JSON input stage. */
final class JsonRowFlattener {

    private JsonRowFlattener() {}

    static List<String> validate(JsonNode root, InputSchemaInfo inputSchema) {
        List<String> violations = new ArrayList<>();
        for (String path : inputSchema.getRequiredFields()) {
            if (isMissingRequired(root, path)) {
                violations.add("missing required field '" + path + "'");
            }
        }
        return violations;
    }

    static boolean isMissingRequired(JsonNode node, String dotPath) {
        String[] parts = dotPath.split("\\.");
        JsonNode current = node;
        for (int i = 0; i < parts.length - 1; i++) {
            current = current.path(parts[i]);
            if (current.isMissingNode()) return false;
        }
        return current.path(parts[parts.length - 1]).isMissingNode();
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

