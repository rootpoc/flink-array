package com.pipeline.common.typeinfo;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Set;

/**
 * Binary {@link TypeSerializer} for Flink's {@link Row} used in named-field mode
 * ({@link Row#withNames()}).
 *
 * <p>Wire format per record:
 * <pre>
 *   [rowKind: byte]     (0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE)
 *   [fieldCount: int]
 *   for each field:
 *     [name:  UTF]
 *     [tag:   byte]     (see TAG_* constants below)
 *     [value: typed]
 * </pre>
 *
 * <p>Value type tags:
 * <ul>
 *   <li>0 = null</li>
 *   <li>1 = String (UTF)</li>
 *   <li>2 = Integer (int)</li>
 *   <li>3 = Long (long)</li>
 *   <li>4 = Double (double)</li>
 *   <li>5 = Float (float)</li>
 *   <li>6 = Boolean (byte: 1=true / 0=false)</li>
 *   <li>7 = BigDecimal (UTF plain string)</li>
 * </ul>
 *
 * <p>Unknown value types fall back to {@code toString()} serialized as a String (tag 1).
 */
public final class FlatRowSerializer extends TypeSerializer<Row> {

    private static final long serialVersionUID = 2L; // bumped: format changed from FlatRow to Row

    // ── Type tags ─────────────────────────────────────────────────────────────

    static final byte TAG_NULL    = 0;
    static final byte TAG_STRING  = 1;
    static final byte TAG_INTEGER = 2;
    static final byte TAG_LONG    = 3;
    static final byte TAG_DOUBLE  = 4;
    static final byte TAG_FLOAT   = 5;
    static final byte TAG_BOOLEAN = 6;
    static final byte TAG_BIGDEC  = 7;

    public static final FlatRowSerializer INSTANCE = new FlatRowSerializer();

    public FlatRowSerializer() {}

    // ── TypeSerializer contract ───────────────────────────────────────────────

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Row> duplicate() {
        return new FlatRowSerializer();
    }

    @Override
    public Row createInstance() {
        return Row.withNames();
    }

    @Override
    public Row copy(Row from) {
        Row copy = Row.withNames(from.getKind());
        Set<String> names = from.getFieldNames(false);
        if (names != null) {
            for (String name : names) {
                copy.setField(name, from.getField(name));
            }
        }
        return copy;
    }

    @Override
    public Row copy(Row from, Row reuse) {
        // Row.withNames() has no efficient in-place reset; create a fresh copy.
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // variable length
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    @Override
    public void serialize(Row record, DataOutputView target) throws IOException {
        target.writeByte(record.getKind().toByteValue());

        Set<String> names = record.getFieldNames(false);
        int size = (names != null) ? names.size() : 0;
        target.writeInt(size);

        if (names != null) {
            for (String name : names) {
                target.writeUTF(name);
                writeTypedValue(record.getField(name), target);
            }
        }
    }

    private static void writeTypedValue(Object value, DataOutputView out) throws IOException {
        if (value == null) {
            out.writeByte(TAG_NULL);
        } else if (value instanceof String) {
            out.writeByte(TAG_STRING);
            out.writeUTF((String) value);
        } else if (value instanceof Integer) {
            out.writeByte(TAG_INTEGER);
            out.writeInt((Integer) value);
        } else if (value instanceof Long) {
            out.writeByte(TAG_LONG);
            out.writeLong((Long) value);
        } else if (value instanceof Double) {
            out.writeByte(TAG_DOUBLE);
            out.writeDouble((Double) value);
        } else if (value instanceof Float) {
            out.writeByte(TAG_FLOAT);
            out.writeFloat((Float) value);
        } else if (value instanceof Boolean) {
            out.writeByte(TAG_BOOLEAN);
            out.writeBoolean((Boolean) value);
        } else if (value instanceof BigDecimal) {
            out.writeByte(TAG_BIGDEC);
            out.writeUTF(((BigDecimal) value).toPlainString());
        } else {
            // Graceful fallback: no data loss
            out.writeByte(TAG_STRING);
            out.writeUTF(value.toString());
        }
    }

    // ── Deserialization ───────────────────────────────────────────────────────

    @Override
    public Row deserialize(DataInputView source) throws IOException {
        RowKind kind = RowKind.fromByteValue(source.readByte());
        int     size = source.readInt();

        Row row = Row.withNames(kind);
        for (int i = 0; i < size; i++) {
            String name  = source.readUTF();
            Object value = readTypedValue(source);
            row.setField(name, value);
        }
        return row;
    }

    @Override
    public Row deserialize(Row reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    private static Object readTypedValue(DataInputView in) throws IOException {
        byte tag = in.readByte();
        switch (tag) {
            case TAG_NULL:
                return null;
            case TAG_STRING:
                return in.readUTF();
            case TAG_INTEGER:
                return in.readInt();
            case TAG_LONG:
                return in.readLong();
            case TAG_DOUBLE:
                return in.readDouble();
            case TAG_FLOAT:
                return in.readFloat();
            case TAG_BOOLEAN:
                return in.readBoolean();
            case TAG_BIGDEC:
                return new BigDecimal(in.readUTF());
            default:
                throw new IOException(
                        "FlatRowSerializer: unknown type tag [" + tag + "]. "
                                + "Data may have been written by a newer serializer version.");
        }
    }

    // ── Bulk copy (source → target without intermediate object) ───────────────

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeByte(source.readByte()); // rowKind

        int size = source.readInt();
        target.writeInt(size);

        for (int i = 0; i < size; i++) {
            target.writeUTF(source.readUTF()); // field name

            byte tag = source.readByte();
            target.writeByte(tag);

            switch (tag) {
                case TAG_NULL:
                    break;
                case TAG_STRING:
                    target.writeUTF(source.readUTF());
                    break;
                case TAG_INTEGER:
                    target.writeInt(source.readInt());
                    break;
                case TAG_LONG:
                    target.writeLong(source.readLong());
                    break;
                case TAG_DOUBLE:
                    target.writeDouble(source.readDouble());
                    break;
                case TAG_FLOAT:
                    target.writeFloat(source.readFloat());
                    break;
                case TAG_BOOLEAN:
                    target.writeBoolean(source.readBoolean());
                    break;
                case TAG_BIGDEC:
                    target.writeUTF(source.readUTF());
                    break;
                default:
                    throw new IOException("Unknown type tag: " + tag);
            }
        }
    }

    // ── Equality / snapshot ───────────────────────────────────────────────────

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FlatRowSerializer;
    }

    @Override
    public int hashCode() {
        return FlatRowSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<Row> snapshotConfiguration() {
        return new FlatRowSerializerSnapshot();
    }
}
