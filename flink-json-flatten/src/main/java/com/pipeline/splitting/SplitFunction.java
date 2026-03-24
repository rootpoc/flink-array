package com.pipeline.splitting;

import com.pipeline.common.PaginationSchema;
import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageTypeInfo;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Flink operator that splits a fully-processed (ingested, transformed)
 * {@link ProcessedMessage} into one {@link ProcessedMessage} per page.
 */
public final class SplitFunction
        extends RichFlatMapFunction<ProcessedMessage, ProcessedMessage>
        implements ResultTypeQueryable<ProcessedMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SplitFunction.class);

    // ── Config ────────────────────────────────────────────────────────────────

    private final PaginationSchema outputSchema;
    private final int              pageSize;
    private final String           splitField;

    // ── Constructor ───────────────────────────────────────────────────────────

    /**
     * @param outputSchema  field-name mapping for pagination metadata
     * @param pageSize      maximum number of array items per output page
     * @param splitField    top-level array key to paginate (e.g. {@code "persons"})
     */
    public SplitFunction(PaginationSchema outputSchema, int pageSize, String splitField) {
        this.outputSchema = outputSchema;
        this.pageSize     = pageSize;
        this.splitField   = splitField;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void open(Configuration parameters) {
        LOG.info("SplitFunction opened: splitField='{}', pageSize={}, schema={}",
                splitField, pageSize, outputSchema);
    }

    // ── Processing ────────────────────────────────────────────────────────────

    @Override
    public void flatMap(ProcessedMessage msg, Collector<ProcessedMessage> out) {
        for (ProcessedMessage splitMessage : split(msg)) {
            out.collect(splitMessage);
        }
    }

    public List<ProcessedMessage> split(ProcessedMessage msg) {
        return SplitPageSupport.split(msg, outputSchema, pageSize, splitField);
    }

    // ── ResultTypeQueryable ───────────────────────────────────────────────────

    @Override
    public TypeInformation<ProcessedMessage> getProducedType() {
        return ProcessedMessageTypeInfo.INSTANCE;
    }
}
