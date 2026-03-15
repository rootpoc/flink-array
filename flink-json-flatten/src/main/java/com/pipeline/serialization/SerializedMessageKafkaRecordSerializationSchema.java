package com.pipeline.serialization;

import com.pipeline.common.SerializedMessage;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import javax.annotation.Nullable;

/** Writes already-serialized Kafka payload bytes produced by the output ProcessFunction. */
public final class SerializedMessageKafkaRecordSerializationSchema
        implements KafkaRecordSerializationSchema<SerializedMessage> {

    private static final long serialVersionUID = 1L;

    private final String outputTopic;

    public SerializedMessageKafkaRecordSerializationSchema(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    @Override
    public void open(org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext ctx,
                     KafkaSinkContext sinkCtx) {
        // no-op
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SerializedMessage msg, KafkaSinkContext ctx, Long timestamp) {
        RecordHeaders headers = new RecordHeaders();
        msg.getHeaders().forEach(headers::add);
        return new ProducerRecord<>(outputTopic, null, timestamp, msg.getKafkaKey(), msg.getValue(), headers);
    }
}

