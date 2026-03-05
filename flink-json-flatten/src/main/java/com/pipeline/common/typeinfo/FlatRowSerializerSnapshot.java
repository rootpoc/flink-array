package com.pipeline.common.typeinfo;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.types.Row;

/**
 * {@link org.apache.flink.api.common.typeutils.TypeSerializerSnapshot} for
 * {@link FlatRowSerializer}.
 *
 * <p>Extends {@link SimpleTypeSerializerSnapshot} because {@link FlatRowSerializer}
 * is stateless — no schema is embedded in the snapshot. Any two instances are always
 * compatible, so Flink will never trigger a legacy copy path during state migration.
 */
public final class FlatRowSerializerSnapshot
        extends SimpleTypeSerializerSnapshot<Row> {

    /** No-arg constructor required by Flink's snapshot restore mechanism. */
    public FlatRowSerializerSnapshot() {
        super(FlatRowSerializer::new);
    }
}
