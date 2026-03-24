package com.pipeline.splitting;

import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.config.PipelineConfig;
import com.pipeline.splitting.schema.SchemaAnalyzer;

import java.util.List;

/** Creates configured {@link SplitFunction} instances from {@link PipelineConfig}. */
public final class SplitFunctionFactory {

    private SplitFunctionFactory() {}

    public static boolean shouldSplitInInput(PipelineConfig config) {
        return config.isSplitEnabled() && config.getSplitStage() == PipelineConfig.SplitStage.INPUT;
    }

    public static boolean shouldSplitInPipeline(PipelineConfig config) {
        return config.isSplitEnabled() && config.getSplitStage() == PipelineConfig.SplitStage.PIPELINE;
    }

    public static SplitFunction create(PipelineConfig config) throws Exception {
        PaginationSchema outputSchema = SchemaAnalyzer.analyze(
                SchemaAnalyzer.loadSchema(config.getOutputSchemaResource()));
        return new SplitFunction(outputSchema, config.getSplittingPageSize(), config.getSplitField());
    }

    public static RuntimeSplitter createRuntime(PipelineConfig config) throws Exception {
        PaginationSchema outputSchema = SchemaAnalyzer.analyze(
                SchemaAnalyzer.loadSchema(config.getOutputSchemaResource()));
        return new RuntimeSplitter(outputSchema, config.getSplittingPageSize(), config.getSplitField());
    }

    /** Lightweight reusable splitter for stages that need page emission without a Flink flatMap operator. */
    public static final class RuntimeSplitter {
        private final PaginationSchema outputSchema;
        private final int pageSize;
        private final String splitField;

        private RuntimeSplitter(PaginationSchema outputSchema, int pageSize, String splitField) {
            this.outputSchema = outputSchema;
            this.pageSize = pageSize;
            this.splitField = splitField;
        }

        public List<ProcessedMessage> split(ProcessedMessage msg) {
            return SplitPageSupport.split(msg, outputSchema, pageSize, splitField);
        }
    }
}
