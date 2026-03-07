package com.pipeline.deserialization;

import com.pipeline.common.DlqRecord;
import com.pipeline.common.typeinfo.FlatRowTypeInfo;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink {@link ProcessFunction} that converts a raw CSV line (UTF-8 bytes) into
 * a named-field {@link Row} using the column order defined in a JSON Schema file.
 *
 * <h2>Column contract</h2>
 * Column names and their order come from
 * {@link SchemaAnalyzer#loadSchema} + {@link SchemaAnalyzer#extractCsvColumns}:
 * the schema's {@code "required"} array defines every column.  All fields are
 * mandatory — the number of parsed values must exactly match the number of schema
 * columns.  A column count mismatch routes the record to {@link #DLQ_TAG}.
 *
 * <h2>Field mapping</h2>
 * Values are mapped positionally: {@code column[i] → row.setField(column[i], value[i])}.
 * An empty string is stored as {@code null} in the Row.
 *
 * <h2>RFC 4180 parsing</h2>
 * <ul>
 *   <li>Fields may be optionally enclosed in double-quotes.</li>
 *   <li>A double-quote inside a quoted field is represented as {@code ""}.</li>
 *   <li>Unquoted fields end at the next comma or end-of-line.</li>
 *   <li>A trailing comma produces a final empty field.</li>
 * </ul>
 *
 * <h2>Error handling</h2>
 * Malformed lines and column-count mismatches are routed to {@link #DLQ_TAG}
 * via side-output.  The main stream never receives failed records; the job does
 * not fail.
 *
 * <h2>Serialization safety</h2>
 * The column list is stored as a plain {@code List<String>} — serialized with the
 * operator graph, no file I/O at worker start-up.
 */
public final class CsvDeserializer
        extends ProcessFunction<byte[], Row>
        implements ResultTypeQueryable<Row> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CsvDeserializer.class);

    /** Side-output tag for records that fail CSV parsing or column-count validation. */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<DlqRecord>("dlq-csv") {};

    private final List<String> columns;

    /**
     * @param schemaPath classpath resource path of the CSV JSON Schema
     *                   (e.g. {@code "schemas/csv-person-flat.schema.json"})
     * @throws IOException if the schema cannot be loaded or is invalid
     */
    public CsvDeserializer(String schemaPath) throws IOException {
        this.columns = SchemaAnalyzer.extractCsvColumns(SchemaAnalyzer.loadSchema(schemaPath));
        LOG.info("CsvDeserializer: columns={}", columns);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void processElement(byte[] bytes, Context ctx, Collector<Row> out) {
        if (bytes == null || bytes.length == 0) {
            ctx.output(DLQ_TAG, DlqRecord.of(
                    bytes != null ? bytes : new byte[0],
                    new IllegalArgumentException("Empty or null CSV message")));
            return;
        }

        try {
            String       line   = new String(bytes, StandardCharsets.UTF_8);
            List<String> values = parseCsvLine(line);

            if (values.size() != columns.size()) {
                throw new IllegalArgumentException(
                        "CSV column count mismatch: expected " + columns.size()
                        + " but got " + values.size());
            }

            Row row = Row.withNames();
            for (int i = 0; i < columns.size(); i++) {
                String v = values.get(i);
                row.setField(columns.get(i), v.isEmpty() ? null : v);
            }
            out.collect(row);

        } catch (Exception e) {
            LOG.warn("Failed to parse CSV ({} bytes): {}", bytes.length, e.getMessage());
            ctx.output(DLQ_TAG, DlqRecord.of(bytes, e));
        }
    }

    // ── RFC 4180 parser ───────────────────────────────────────────────────────

    /**
     * Parses one CSV line into a list of raw string values.
     *
     * <ul>
     *   <li>Quoted fields: opening/closing {@code "} stripped; {@code ""} → {@code "}.</li>
     *   <li>Unquoted fields: value between commas, not trimmed.</li>
     *   <li>Trailing comma: final field is an empty string.</li>
     * </ul>
     */
    static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        int i   = 0;
        int len = line.length();

        while (true) {
            if (i < len && line.charAt(i) == '"') {
                // Quoted field — consume until unescaped closing quote
                StringBuilder sb = new StringBuilder();
                i++; // skip opening quote
                while (i < len) {
                    char c = line.charAt(i++);
                    if (c == '"') {
                        if (i < len && line.charAt(i) == '"') {
                            sb.append('"'); // escaped double-quote
                            i++;
                        } else {
                            break; // closing quote consumed
                        }
                    } else {
                        sb.append(c);
                    }
                }
                fields.add(sb.toString());
                if (i < len && line.charAt(i) == ',') {
                    i++; // skip comma
                    if (i == len) {
                        fields.add(""); // trailing comma → empty final field
                        break;
                    }
                } else {
                    break; // end of line after quoted field
                }
            } else {
                // Unquoted field — read until next comma or end
                int start = i;
                while (i < len && line.charAt(i) != ',') i++;
                fields.add(line.substring(start, i));
                if (i < len) {
                    i++; // skip comma
                    if (i == len) {
                        fields.add(""); // trailing comma → empty final field
                        break;
                    }
                } else {
                    break; // end of line, no trailing comma
                }
            }
        }

        return fields;
    }

    // ── ResultTypeQueryable ───────────────────────────────────────────────────

    @Override
    public TypeInformation<Row> getProducedType() {
        return FlatRowTypeInfo.INSTANCE;
    }

    /** Returns the ordered column list loaded from the schema (useful for testing). */
    public List<String> getColumns() {
        return columns;
    }
}
