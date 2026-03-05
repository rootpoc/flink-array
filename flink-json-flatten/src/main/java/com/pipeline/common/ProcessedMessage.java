package com.pipeline.common;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Envelope that carries Kafka metadata alongside the business payload ({@link Row})
 * through every operator in the pipeline.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>{@code KafkaEnvelopeDeserializer} creates the message with {@code payload = null}
 *       and populates {@code kafkaKey}, {@code originalBytes}, and {@code headers}.</li>
 *   <li>{@code ValidateSplitFlattenFunction} calls {@link #withPayload(Row)} to attach
 *       the flattened Row and emits one {@code ProcessedMessage} per page.</li>
 *   <li>Downstream operators ({@code FieldMappingFunction}, {@code UpperCaseMapFunction})
 *       call {@link #withPayload(Row)} again to attach a transformed Row while preserving
 *       all metadata fields unchanged.</li>
 *   <li>{@code ReconstructSerializer} serialises the payload to JSON and copies
 *       {@code kafkaKey} and {@code headers} onto the output {@code ProducerRecord}.</li>
 * </ol>
 *
 * <h2>DLQ</h2>
 * Any operator that routes a message to the DLQ passes {@code this} to
 * {@link DlqRecord#of(ProcessedMessage, Throwable)} so the DLQ record retains
 * the original Kafka key, headers, and raw bytes.
 */
public final class ProcessedMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Kafka record key — may be {@code null} if the producer did not set one. */
    private final byte[] kafkaKey;

    /** Raw Kafka value bytes captured at the source — preserved for DLQ use in any operator. */
    private final byte[] originalBytes;

    /**
     * Kafka message headers as a name → value map.
     * When multiple headers share the same name, the last value wins (standard Map semantics).
     * Never {@code null} — may be {@link Collections#emptyMap()}.
     */
    private final Map<String, byte[]> headers;

    /**
     * Business payload — {@code null} at the source, set by
     * {@code ValidateSplitFlattenFunction} and replaced by each downstream transformer.
     */
    private final Row payload;

    public ProcessedMessage(
            byte[] kafkaKey,
            byte[] originalBytes,
            Map<String, byte[]> headers,
            Row payload) {
        this.kafkaKey      = kafkaKey;
        this.originalBytes = originalBytes;
        this.headers       = (headers != null) ? Collections.unmodifiableMap(headers)
                                               : Collections.emptyMap();
        this.payload       = payload;
    }

    /**
     * Convenience factory for tests — no Kafka key, no headers, no payload.
     * The supplied bytes are treated as the original Kafka value.
     */
    public static ProcessedMessage ofValue(byte[] value) {
        return new ProcessedMessage(null, value, Map.of(), null);
    }

    /** Convenience factory for tests — no Kafka key, no headers, empty originalBytes. */
    public static ProcessedMessage ofPayload(Row payload) {
        return new ProcessedMessage(null, new byte[0], Map.of(), payload);
    }

    /**
     * Returns a new {@code ProcessedMessage} with the given payload,
     * keeping {@code kafkaKey}, {@code originalBytes}, and {@code headers} unchanged.
     */
    public ProcessedMessage withPayload(Row payload) {
        return new ProcessedMessage(kafkaKey, originalBytes, headers, payload);
    }

    public byte[]              getKafkaKey()      { return kafkaKey; }
    public byte[]              getOriginalBytes()  { return originalBytes; }
    public Map<String, byte[]> getHeaders()        { return headers; }
    public Row                 getPayload()         { return payload; }
}
