package com.pipeline.common.typeinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Row;

/**
 * Flink {@link TypeInformation} for Flink's {@link Row} used in named-field mode
 * ({@link Row#withNames()}) within the JSON-flatten pipeline.
 *
 * <p>Returned by {@link com.pipeline.deserialization.FlatteningDeserializer} via
 * {@code ResultTypeQueryable.getProducedType()}, ensuring that Flink's type
 * extraction uses {@link FlatRowSerializer} — not Kryo — for network transfers
 * between operators.
 *
 * <p>Why a custom TypeInfo instead of {@code Types.ROW}?
 * Flink's built-in {@code RowTypeInfo} requires fixed, compile-time field names and
 * types. Our JSON schema is dynamic (field names are discovered at runtime during
 * flattening), so we provide a stateless TypeInfo that delegates serialization to
 * {@link FlatRowSerializer}, which handles any set of named fields.
 */
public final class FlatRowTypeInfo extends TypeInformation<Row> {

    private static final long serialVersionUID = 1L;

    public static final FlatRowTypeInfo INSTANCE = new FlatRowTypeInfo();

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        // Treated as a single opaque field from Flink's perspective;
        // actual arity is dynamic and known only at runtime.
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<Row> getTypeClass() {
        return Row.class;
    }

    @Override
    public boolean isKeyType() {
        // Named rows cannot be used as KeyedStream keys (no stable hash over dynamic fields).
        return false;
    }

    @Override
    public TypeSerializer<Row> createSerializer(ExecutionConfig config) {
        return new FlatRowSerializer();
    }

    @Override
    public String toString() {
        return "FlatRowTypeInfo(Row.withNames)";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FlatRowTypeInfo;
    }

    @Override
    public int hashCode() {
        return FlatRowTypeInfo.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof FlatRowTypeInfo;
    }
}
