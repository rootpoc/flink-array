package com.pipeline.splitting;

import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Shared page-splitting logic used by both SplitFunction and input-stage runtime splitting. */
final class SplitPageSupport {

    private SplitPageSupport() {}

    static List<ProcessedMessage> split(ProcessedMessage msg,
                                        PaginationSchema outputSchema,
                                        int pageSize,
                                        String splitField) {
        Row row = msg.getPayload();
        if (row == null) {
            return List.of(msg);
        }

        Set<String> allNames = row.getFieldNames(false);
        if (allNames == null) {
            return List.of(msg);
        }

        String arrayPrefix = splitField + ".";
        int maxIndex = -1;
        for (String name : allNames) {
            if (!name.startsWith(arrayPrefix)) continue;
            String rest = name.substring(arrayPrefix.length());
            int dotPos = rest.indexOf('.');
            String indexStr = dotPos >= 0 ? rest.substring(0, dotPos) : rest;
            try {
                int idx = Integer.parseInt(indexStr);
                if (idx > maxIndex) maxIndex = idx;
            } catch (NumberFormatException ignored) {
                // not an indexed array-item key
            }
        }

        if (maxIndex < 0) {
            return List.of(msg);
        }

        int arraySize = maxIndex + 1;
        int totalPages = (int) Math.ceil((double) arraySize / pageSize);

        List<String> sharedKeys = new ArrayList<>();
        for (String name : allNames) {
            if (name.startsWith(arrayPrefix)) {
                String rest = name.substring(arrayPrefix.length());
                int dotPos = rest.indexOf('.');
                String indexStr = dotPos >= 0 ? rest.substring(0, dotPos) : rest;
                try {
                    Integer.parseInt(indexStr);
                    continue;
                } catch (NumberFormatException ignored) {
                    // treat non-indexed names as shared
                }
            }
            sharedKeys.add(name);
        }

        List<ProcessedMessage> pages = new ArrayList<>(totalPages);
        for (int p = 0; p < totalPages; p++) {
            int start = p * pageSize;
            int end = Math.min(start + pageSize, arraySize);
            int count = end - start;

            Row pageRow = Row.withNames();
            for (String key : sharedKeys) {
                pageRow.setField(key, row.getField(key));
            }

            pageRow.setField(outputSchema.getIndexFieldName(), p);
            pageRow.setField(outputSchema.getTotalFieldName(), totalPages);
            pageRow.setField(outputSchema.getCountFieldName(), count);

            for (int i = start; i < end; i++) {
                int pageLocalIndex = i - start;
                String srcPrefix = arrayPrefix + i + ".";
                String dstPrefix = arrayPrefix + pageLocalIndex + ".";

                for (String name : allNames) {
                    if (name.startsWith(srcPrefix)) {
                        String suffix = name.substring(srcPrefix.length());
                        pageRow.setField(dstPrefix + suffix, row.getField(name));
                    } else if (name.equals(arrayPrefix + i)) {
                        pageRow.setField(arrayPrefix + pageLocalIndex, row.getField(name));
                    }
                }
            }

            pages.add(msg.withPayload(pageRow));
        }
        return pages;
    }
}

