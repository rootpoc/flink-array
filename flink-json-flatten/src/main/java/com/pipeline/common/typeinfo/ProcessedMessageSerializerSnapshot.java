package com.pipeline.common.typeinfo;

import com.pipeline.common.ProcessedMessage;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;

/**
 * {@link org.apache.flink.api.common.typeutils.TypeSerializerSnapshot} for
 * {@link ProcessedMessageSerializer}.
 *
 * <p>Extends {@link SimpleTypeSerializerSnapshot} because {@link ProcessedMessageSerializer}
 * is stateless — no schema is embedded in the snapshot.
 */
public final class ProcessedMessageSerializerSnapshot
        extends SimpleTypeSerializerSnapshot<ProcessedMessage> {

    public ProcessedMessageSerializerSnapshot() {
        super(ProcessedMessageSerializer::new);
    }
}
