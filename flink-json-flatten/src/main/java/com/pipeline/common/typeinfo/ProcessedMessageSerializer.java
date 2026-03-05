package com.pipeline.common.typeinfo;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binary {@link TypeSerializer} for {@link ProcessedMessage}.
 *
 * <p>Wire format per record:
 * <pre>
 *   [hasKey:           boolean]
 *   if hasKey:
 *     [keyLen:         int]
 *     [key:            bytes]
 *   [originalBytesLen: int]
 *   [originalBytes:    bytes]
 *   [headersCount:     int]
 *   for each header:
 *     [name:           UTF]
 *     [valueLen:       int]
 *     [value:          bytes]
 *   [hasPayload:       boolean]
 *   if hasPayload:
 *     [Row:            FlatRowSerializer format]
 * </pre>
 *
 * <p>Metadata fields (kafkaKey, originalBytes, headers) are written verbatim;
 * the Row payload is delegated to {@link FlatRowSerializer}.
 */
public final class ProcessedMessageSerializer extends TypeSerializer<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    public static final ProcessedMessageSerializer INSTANCE = new ProcessedMessageSerializer();

    private static final FlatRowSerializer ROW_SER = FlatRowSerializer.INSTANCE;

    public ProcessedMessageSerializer() {}

    @Override
    public boolean isImmutableType() {
        return true; // ProcessedMessage is immutable; Row payload is mutable but treated as value
    }

    @Override
    public TypeSerializer<ProcessedMessage> duplicate() {
        return new ProcessedMessageSerializer();
    }

    @Override
    public ProcessedMessage createInstance() {
        return ProcessedMessage.ofValue(new byte[0]);
    }

    @Override
    public ProcessedMessage copy(ProcessedMessage from) {
        Row payloadCopy = (from.getPayload() != null) ? ROW_SER.copy(from.getPayload()) : null;
        return new ProcessedMessage(from.getKafkaKey(), from.getOriginalBytes(), from.getHeaders(), payloadCopy);
    }

    @Override
    public ProcessedMessage copy(ProcessedMessage from, ProcessedMessage reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // variable length
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    @Override
    public void serialize(ProcessedMessage record, DataOutputView target) throws IOException {
        writeBytes(record.getKafkaKey(), target, true);
        writeBytes(record.getOriginalBytes(), target, false);
        writeHeaders(record.getHeaders(), target);
        writePayload(record.getPayload(), target);
    }

    private static void writeBytes(byte[] bytes, DataOutputView target, boolean withPresenceFlag)
            throws IOException {
        if (withPresenceFlag) {
            if (bytes != null) {
                target.writeBoolean(true);
                target.writeInt(bytes.length);
                target.write(bytes);
            } else {
                target.writeBoolean(false);
            }
        } else {
            int len = (bytes != null) ? bytes.length : 0;
            target.writeInt(len);
            if (len > 0) target.write(bytes);
        }
    }

    private static void writeHeaders(Map<String, byte[]> headers, DataOutputView target)
            throws IOException {
        target.writeInt(headers.size());
        for (Map.Entry<String, byte[]> e : headers.entrySet()) {
            target.writeUTF(e.getKey());
            byte[] val = e.getValue();
            int vLen = (val != null) ? val.length : 0;
            target.writeInt(vLen);
            if (vLen > 0) target.write(val);
        }
    }

    private static void writePayload(Row payload, DataOutputView target) throws IOException {
        if (payload != null) {
            target.writeBoolean(true);
            ROW_SER.serialize(payload, target);
        } else {
            target.writeBoolean(false);
        }
    }

    // ── Deserialization ───────────────────────────────────────────────────────

    @Override
    public ProcessedMessage deserialize(DataInputView source) throws IOException {
        byte[] key      = readBytesWithFlag(source);
        byte[] original = readBytes(source);
        Map<String, byte[]> headers = readHeaders(source);
        Row payload     = readPayload(source);
        return new ProcessedMessage(key, original, headers, payload);
    }

    @Override
    public ProcessedMessage deserialize(ProcessedMessage reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    private static byte[] readBytesWithFlag(DataInputView source) throws IOException {
        if (!source.readBoolean()) return null;
        return readBytes(source);
    }

    private static byte[] readBytes(DataInputView source) throws IOException {
        int len = source.readInt();
        if (len == 0) return new byte[0];
        byte[] buf = new byte[len];
        source.readFully(buf);
        return buf;
    }

    private static Map<String, byte[]> readHeaders(DataInputView source) throws IOException {
        int count = source.readInt();
        Map<String, byte[]> headers = new LinkedHashMap<>(count * 2);
        for (int i = 0; i < count; i++) {
            String name = source.readUTF();
            headers.put(name, readBytes(source));
        }
        return headers;
    }

    private static Row readPayload(DataInputView source) throws IOException {
        return source.readBoolean() ? ROW_SER.deserialize(source) : null;
    }

    // ── Bulk copy (source → target without heap allocation for payload) ────────

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        copyBytesWithFlag(source, target);  // kafkaKey
        copyBytes(source, target);          // originalBytes
        copyHeaders(source, target);        // headers
        copyPayload(source, target);        // payload
    }

    private static void copyBytesWithFlag(DataInputView source, DataOutputView target)
            throws IOException {
        boolean present = source.readBoolean();
        target.writeBoolean(present);
        if (present) copyBytes(source, target);
    }

    private static void copyBytes(DataInputView source, DataOutputView target) throws IOException {
        int len = source.readInt();
        target.writeInt(len);
        if (len > 0) {
            byte[] buf = new byte[len];
            source.readFully(buf);
            target.write(buf);
        }
    }

    private static void copyHeaders(DataInputView source, DataOutputView target) throws IOException {
        int count = source.readInt();
        target.writeInt(count);
        for (int i = 0; i < count; i++) {
            target.writeUTF(source.readUTF());
            copyBytes(source, target);
        }
    }

    private static void copyPayload(DataInputView source, DataOutputView target) throws IOException {
        boolean present = source.readBoolean();
        target.writeBoolean(present);
        if (present) ROW_SER.copy(source, target);
    }

    // ── Equality / snapshot ───────────────────────────────────────────────────

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ProcessedMessageSerializer;
    }

    @Override
    public int hashCode() {
        return ProcessedMessageSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<ProcessedMessage> snapshotConfiguration() {
        return new ProcessedMessageSerializerSnapshot();
    }
}
