package com.pipeline;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import com.pipeline.config.PipelineConfig;
import com.pipeline.deserialization.CsvDeserializer;
import com.pipeline.splitting.SplitFunctionFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/** CSV-specific ingress ProcessFunction. */
public final class CsvInputProcessFunction
        extends ProcessFunction<ProcessedMessage, ProcessedMessage>
        implements ResultTypeQueryable<ProcessedMessage> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CsvInputProcessFunction.class);
    private final PipelineConfig config;
    private transient CsvDeserializer deserializer;
    private transient SplitFunctionFactory.RuntimeSplitter inputSplitter;
    public CsvInputProcessFunction(PipelineConfig config) {
        this.config = config;
    }
    @Override
    public void open(Configuration params) throws Exception {
        deserializer = new CsvDeserializer(config.getInputSchemaResource());
        inputSplitter = SplitFunctionFactory.shouldSplitInInput(config)
                ? SplitFunctionFactory.createRuntime(config)
                : null;
        LOG.info("CsvInputProcessFunction opened: schema='{}'", config.getInputSchemaResource());
    }
    @Override
    public void processElement(ProcessedMessage msg, Context ctx, Collector<ProcessedMessage> out) {
        byte[] bytes = msg.getOriginalBytes();
        if (bytes == null || bytes.length == 0) {
            ctx.output(InputProcessFunctionFactory.DLQ_TAG,
                    DlqRecord.of(msg, new IllegalArgumentException("Empty or null message")));
            return;
        }
        try {
            Row row = deserializer.deserialize(bytes);
            ProcessedMessage processed = msg.withPayload(row);
            if (inputSplitter != null) {
                for (ProcessedMessage splitMessage : inputSplitter.split(processed)) {
                    out.collect(splitMessage);
                }
            } else {
                out.collect(processed);
            }
        } catch (Exception e) {
            LOG.warn("Failed to process CSV message ({} bytes): {}", bytes.length, e.getMessage());
            ctx.output(InputProcessFunctionFactory.DLQ_TAG, DlqRecord.of(msg, e));
        }
    }
    @Override
    public TypeInformation<ProcessedMessage> getProducedType() {
        return ProcessedMessageTypeInfo.INSTANCE;
    }
}
