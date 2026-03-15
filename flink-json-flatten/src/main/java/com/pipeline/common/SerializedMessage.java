package com.pipeline.common;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Final serialized payload ready to be written to Kafka.
 */
public final class SerializedMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] kafkaKey;
    private final byte[] value;
    private final Map<String, byte[]> headers;

    public SerializedMessage(byte[] kafkaKey, byte[] value, Map<String, byte[]> headers) {
        this.kafkaKey = kafkaKey;
        this.value = value;
        this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
    }

    public byte[] getKafkaKey() {
        return kafkaKey;
    }

    public byte[] getValue() {
        return value;
    }

    public Map<String, byte[]> getHeaders() {
        return headers;
    }
}

