package com.pipeline.common.typeinfo;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Read-only binary decoder for {@link ProcessedMessage}. */
final class ProcessedMessageReader {

    private static final FlatRowSerializer ROW_SER = FlatRowSerializer.INSTANCE;

    ProcessedMessage read(DataInputView source) throws IOException {
        byte[] key = readBytesWithFlag(source);
        byte[] original = readBytes(source);
        Map<String, byte[]> headers = readHeaders(source);
        Row payload = readPayload(source);
        return new ProcessedMessage(key, original, headers, payload);
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
}

