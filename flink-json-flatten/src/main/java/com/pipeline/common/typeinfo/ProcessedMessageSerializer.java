package com.pipeline.common.typeinfo;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;

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

    private final ProcessedMessageWriter writer;
    private final ProcessedMessageReader reader;

    public ProcessedMessageSerializer() {
        this.writer = new ProcessedMessageWriter();
        this.reader = new ProcessedMessageReader();
    }

    @Override
    public boolean isImmutableType() {
        return true;
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
        return -1;
    }

    @Override
    public void serialize(ProcessedMessage record, DataOutputView target) throws IOException {
        writer.write(record, target);
    }

    @Override
    public ProcessedMessage deserialize(DataInputView source) throws IOException {
        return reader.read(source);
    }

    @Override
    public ProcessedMessage deserialize(ProcessedMessage reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    // ── Bulk copy (source → target without heap allocation for payload) ────────

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        copyBytesWithFlag(source, target);
        copyBytes(source, target);
        copyHeaders(source, target);
        copyPayload(source, target);
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
