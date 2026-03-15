package com.pipeline;

import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.SerializedMessage;
import com.pipeline.config.PipelineConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

/** Chooses the configured egress serialization ProcessFunction implementation. */
public final class OutputProcessFunctionFactory {

    private OutputProcessFunctionFactory() {}

    public static final OutputTag<DlqRecord> DLQ_TAG = new OutputTag<DlqRecord>("dlq-output") {};

    public static ProcessFunction<ProcessedMessage, SerializedMessage> create(PipelineConfig config) {
        switch (config.getOutputFormat()) {
            case JSON:
                return new JsonOutputProcessFunction(config);
            case CSV:
                return new CsvOutputProcessFunction(config);
            default:
                throw new IllegalStateException("Unsupported output format: " + config.getOutputFormat());
        }
    }
}

