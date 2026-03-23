package com.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import com.pipeline.config.PipelineConfig;
import com.pipeline.splitting.schema.SchemaAnalyzer;
import com.pipeline.validation.InputSchemaInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSON-specific ingress ProcessFunction. */
public final class JsonInputProcessFunction
        extends ProcessFunction<ProcessedMessage, ProcessedMessage>
        implements ResultTypeQueryable<ProcessedMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonInputProcessFunction.class);

    private final PipelineConfig config;
    private transient ThreadLocal<ObjectMapper> mapperLocal;
    private transient InputSchemaInfo inputSchema;

    public JsonInputProcessFunction(PipelineConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration params) throws Exception {
        mapperLocal = ThreadLocal.withInitial(ObjectMapper::new);
        JsonNode schema = SchemaAnalyzer.loadSchema(config.getInputSchemaResource());
        inputSchema = InputSchemaInfo.analyze(schema);
        LOG.info("JsonInputProcessFunction opened: schema='{}', nullHandling={}",
                config.getInputSchemaResource(), config.getNullHandling());
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
            JsonNode root = mapperLocal.get().readTree(bytes);
            var violations = JsonRowFlattener.validate(root, inputSchema);
            if (!violations.isEmpty()) {
                ctx.output(InputProcessFunctionFactory.DLQ_TAG,
                        DlqRecord.of(msg, new IllegalArgumentException("Input schema validation failed: " + violations)));
                return;
            }

            Row row = Row.withNames();
            JsonRowFlattener.flattenInto(root, "", row, config.getNullHandling());
            out.collect(msg.withPayload(row));
        } catch (Exception e) {
            LOG.warn("Failed to process JSON message ({} bytes): {}", bytes.length, e.getMessage());
            ctx.output(InputProcessFunctionFactory.DLQ_TAG, DlqRecord.of(msg, e));
        }
    }

    @Override
    public TypeInformation<ProcessedMessage> getProducedType() {
        return ProcessedMessageTypeInfo.INSTANCE;
    }
}
