package com.pipeline.deserialization;

import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV deserializer backed by a JSON schema whose {@code required} array defines
 * the exact column order.
 *
 * <p>Contract:
 * <ul>
 *   <li>All fields are mandatory.</li>
 *   <li>Column order is taken from {@link SchemaAnalyzer#extractCsvColumns}.</li>
 *   <li>Delimiter is comma ({@code ,}).</li>
 * </ul>
 */
public final class CsvDeserializer implements DeserializationSchema<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvDeserializer.class);

    private final String schemaPath;
    private final List<String> columns;

    public CsvDeserializer(String schemaPath) throws IOException {
        this.schemaPath = schemaPath;
        this.columns = SchemaAnalyzer.extractCsvColumns(SchemaAnalyzer.loadSchema(schemaPath));
        LOG.info("CsvDeserializer opened: schemaPath='{}', columns={}", schemaPath, columns);
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            throw new IOException("CSV input is empty");
        }

        String line = new String(message, StandardCharsets.UTF_8).strip();
        List<String> values = parseCsvLine(line);
        if (values.size() != columns.size()) {
            throw new IOException("Expected " + columns.size() + " CSV fields but got " + values.size());
        }

        Row row = Row.withNames();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String raw = values.get(i);
            row.setField(column, coerce(raw, column));
        }
        return row;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }

    public List<String> getColumns() {
        return columns;
    }

    static Object coerce(String raw, String column) {
        if (raw == null) return null;
        if ("age".equals(column)) {
            return Long.parseLong(raw);
        }
        return raw;
    }

    /**
     * Minimal CSV parser supporting commas, double quotes, escaped quotes, and
     * embedded newlines inside quoted fields.
     */
    public static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++; // consume escaped quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields;
    }
}

