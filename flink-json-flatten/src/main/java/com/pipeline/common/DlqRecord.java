package com.pipeline.common;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Envelope for messages that could not be parsed or flattened.
 *
 * <p>Written to the DLQ Kafka topic via {@link com.pipeline.serialization.DlqSerializationSchema}.
 * Retains the original bytes so the message can be replayed or inspected.
 */
public final class DlqRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Raw bytes of the Kafka message that failed processing. */
    private final byte[] originalBytes;

    /** Human-readable description of the failure. */
    private final String errorMessage;

    /** Fully-qualified class name of the exception (e.g. {@code com.fasterxml.jackson.core.JsonParseException}). */
    private final String errorClass;

    /** Epoch-millisecond timestamp when the error was caught. */
    private final long errorTimestampMs;

    /** Kafka topic the message was consumed from. */
    private final String sourceTopic;

    /** Kafka partition (may be -1 if unknown). */
    private final int sourcePartition;

    /** Kafka offset (may be -1 if unknown). */
    private final long sourceOffset;

    public DlqRecord(
            byte[] originalBytes,
            String errorMessage,
            String errorClass,
            long errorTimestampMs,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset) {
        this.originalBytes    = originalBytes;
        this.errorMessage     = errorMessage;
        this.errorClass       = errorClass;
        this.errorTimestampMs = errorTimestampMs;
        this.sourceTopic      = sourceTopic;
        this.sourcePartition  = sourcePartition;
        this.sourceOffset     = sourceOffset;
    }

    /** Convenience factory — preserves the original Kafka bytes from the envelope. */
    public static DlqRecord of(ProcessedMessage msg, Throwable cause) {
        return of(msg.getOriginalBytes(), cause);
    }

    /** Convenience factory — use when Kafka metadata is unavailable. */
    public static DlqRecord of(byte[] originalBytes, Throwable cause) {
        return new DlqRecord(
                originalBytes,
                cause.getMessage() != null ? cause.getMessage() : cause.toString(),
                cause.getClass().getName(),
                System.currentTimeMillis(),
                "unknown",
                -1,
                -1L
        );
    }

    public byte[] getOriginalBytes()    { return originalBytes; }
    public String getErrorMessage()     { return errorMessage; }
    public String getErrorClass()       { return errorClass; }
    public long getErrorTimestampMs()   { return errorTimestampMs; }
    public String getSourceTopic()      { return sourceTopic; }
    public int getSourcePartition()     { return sourcePartition; }
    public long getSourceOffset()       { return sourceOffset; }

    @Override
    public String toString() {
        return "DlqRecord{"
                + "errorClass='" + errorClass + '\''
                + ", errorMessage='" + errorMessage + '\''
                + ", sourceTopic='" + sourceTopic + '\''
                + ", partition=" + sourcePartition
                + ", offset=" + sourceOffset
                + ", bytesLen=" + (originalBytes != null ? originalBytes.length : 0)
                + '}';
    }
}
