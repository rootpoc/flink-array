package com.pipeline.deserialization;

import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link KafkaRecordDeserializationSchema} that captures the full Kafka record envelope
 * — key, value bytes, and headers — into a {@link ProcessedMessage} with a {@code null} payload.
 *
 * <p>Downstream, {@link com.pipeline.ValidateSplitFlattenFunction} fills in the payload
 * by calling {@link ProcessedMessage#withPayload(org.apache.flink.types.Row)}.
 *
 * <p>When multiple Kafka headers share the same name, the last value wins
 * (consistent with {@link ProcessedMessage}'s Map semantics).
 */
public final class KafkaEnvelopeDeserializer
        implements KafkaRecordDeserializationSchema<ProcessedMessage> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<ProcessedMessage> out) throws IOException {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (Header h : record.headers()) {
            headers.put(h.key(), h.value());
        }
        out.collect(new ProcessedMessage(
                record.key(),
                record.value(),
                headers,
                null));
    }

    @Override
    public TypeInformation<ProcessedMessage> getProducedType() {
        return ProcessedMessageTypeInfo.INSTANCE;
    }
}
