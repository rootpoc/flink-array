package com.pipeline;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.SerializedMessage;
import com.pipeline.config.PipelineConfig;
import com.pipeline.serialization.CsvSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.LinkedHashMap;
import java.util.Map;
/** CSV-specific output ProcessFunction that serializes the Row payload into CSV bytes. */
public final class CsvOutputProcessFunction
        extends ProcessFunction<ProcessedMessage, SerializedMessage>
        implements ResultTypeQueryable<SerializedMessage> {
    private static final long serialVersionUID = 1L;
    private final PipelineConfig config;
    private transient CsvSerializer serializer;
    public CsvOutputProcessFunction(PipelineConfig config) {
        this.config = config;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        serializer = new CsvSerializer(config.getOutputTopic(), config.getOutputSchemaResource());
        serializer.open(null, null);
    }
    @Override
    public void processElement(ProcessedMessage msg, Context ctx, Collector<SerializedMessage> out) {
        try {
            ProducerRecord<byte[], byte[]> record = serializer.serialize(msg, null, 0L);
            if (record == null) {
                throw new IllegalStateException("CSV serializer returned null ProducerRecord");
            }
            Map<String, byte[]> headers = new LinkedHashMap<>();
            record.headers().forEach(h -> headers.put(h.key(), h.value()));
            out.collect(new SerializedMessage(record.key(), record.value(), headers));
        } catch (Exception e) {
            ctx.output(OutputProcessFunctionFactory.DLQ_TAG, DlqRecord.of(msg, e));
        }
    }
    @Override
    public TypeInformation<SerializedMessage> getProducedType() {
        return TypeInformation.of(SerializedMessage.class);
    }
}
