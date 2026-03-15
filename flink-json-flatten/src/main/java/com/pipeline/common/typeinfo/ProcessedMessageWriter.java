package com.pipeline.common.typeinfo;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;

/** Write-only binary encoder for {@link ProcessedMessage}. */
final class ProcessedMessageWriter {

    private static final FlatRowSerializer ROW_SER = FlatRowSerializer.INSTANCE;

    void write(ProcessedMessage record, DataOutputView target) throws IOException {
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
}

