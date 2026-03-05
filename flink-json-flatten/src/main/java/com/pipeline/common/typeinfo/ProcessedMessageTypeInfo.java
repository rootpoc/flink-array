package com.pipeline.common.typeinfo;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Flink {@link TypeInformation} for {@link ProcessedMessage} — the envelope that carries
 * Kafka metadata alongside the business {@link org.apache.flink.types.Row} payload
 * through every operator in the pipeline.
 *
 * <p>Returned by {@link com.pipeline.deserialization.KafkaEnvelopeDeserializer} and each
 * operator via {@code ResultTypeQueryable.getProducedType()}, ensuring Flink uses
 * {@link ProcessedMessageSerializer} — not Kryo — for all inter-operator transfers.
 */
public final class ProcessedMessageTypeInfo extends TypeInformation<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    public static final ProcessedMessageTypeInfo INSTANCE = new ProcessedMessageTypeInfo();

    @Override public boolean isBasicType()  { return false; }
    @Override public boolean isTupleType()  { return false; }
    @Override public int     getArity()     { return 1; }
    @Override public int     getTotalFields() { return 1; }
    @Override public Class<ProcessedMessage> getTypeClass() { return ProcessedMessage.class; }
    @Override public boolean isKeyType()    { return false; }

    @Override
    public TypeSerializer<ProcessedMessage> createSerializer(ExecutionConfig config) {
        return new ProcessedMessageSerializer();
    }

    @Override public String toString()  { return "ProcessedMessageTypeInfo"; }
    @Override public boolean equals(Object obj) { return obj instanceof ProcessedMessageTypeInfo; }
    @Override public int     hashCode() { return ProcessedMessageTypeInfo.class.hashCode(); }
    @Override public boolean canEqual(Object obj) { return obj instanceof ProcessedMessageTypeInfo; }
}
