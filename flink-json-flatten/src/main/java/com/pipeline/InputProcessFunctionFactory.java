package com.pipeline;

import com.pipeline.common.DlqRecord;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.config.PipelineConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;

/** Chooses the configured ingress ProcessFunction implementation. */
public final class InputProcessFunctionFactory {

    private InputProcessFunctionFactory() {}

    public static final OutputTag<DlqRecord> DLQ_TAG = new OutputTag<DlqRecord>("dlq-input") {};

    public static ProcessFunction<ProcessedMessage, ProcessedMessage> create(PipelineConfig config) {
        switch (config.getInputFormat()) {
            case JSON:
                return new JsonInputProcessFunction(config);
            case CSV:
                return new CsvInputProcessFunction(config);
            default:
                throw new IllegalStateException("Unsupported input format: " + config.getInputFormat());
        }
    }
}

