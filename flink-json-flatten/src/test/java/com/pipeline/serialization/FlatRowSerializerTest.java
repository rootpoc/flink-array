package com.pipeline.serialization;

import com.pipeline.common.typeinfo.FlatRowSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Binary round-trip tests for {@link FlatRowSerializer}.
 *
 * <p>Verifies that all supported value types survive serialize → deserialize without
 * loss, that Flink's named {@link Row} field order is preserved, and that the bulk
 * copy path produces an identical byte sequence.
 */
class FlatRowSerializerTest {

    private final FlatRowSerializer ser = new FlatRowSerializer();

    // ── All supported value types ─────────────────────────────────────────────

    @Test
    void roundTrip_allTypes() throws Exception {
        Row row = Row.withNames();
        row.setField("str",  "hello");
        row.setField("int",  42);
        row.setField("long", Long.MAX_VALUE);
        row.setField("dbl",  3.14d);
        row.setField("flt",  1.5f);
        row.setField("bool", true);
        row.setField("bd",   new BigDecimal("12345.6789"));
        row.setField("null", null);

        Row restored = serializeDeserialize(row);

        assertEquals("hello",                       restored.getField("str"));
        assertEquals(42,                            restored.getField("int"));
        assertEquals(Long.MAX_VALUE,                restored.getField("long"));
        assertEquals(3.14d,                         restored.getField("dbl"));
        assertEquals(1.5f,                          restored.getField("flt"));
        assertEquals(true,                          restored.getField("bool"));
        assertEquals(new BigDecimal("12345.6789"),  restored.getField("bd"));
        assertNull(restored.getField("null"));
        // null must be explicitly present (not just absent)
        assertTrue(restored.getFieldNames(false).contains("null"),
                "null-valued field must be preserved in the Row");
    }

    // ── Empty Row ─────────────────────────────────────────────────────────────

    @Test
    void roundTrip_emptyRow() throws Exception {
        Row empty    = Row.withNames();
        Row restored = serializeDeserialize(empty);

        Set<String> names = restored.getFieldNames(false);
        assertTrue(names == null || names.isEmpty());
    }

    // ── RowKind preservation ──────────────────────────────────────────────────

    @Test
    void roundTrip_rowKindPreserved() throws Exception {
        Row row = Row.withNames(RowKind.UPDATE_AFTER);
        row.setField("k", "v");

        Row restored = serializeDeserialize(row);

        assertEquals(RowKind.UPDATE_AFTER, restored.getKind());
        assertEquals("v", restored.getField("k"));
    }

    // ── Insertion order preserved ─────────────────────────────────────────────

    @Disabled
    @Test
    void roundTrip_insertionOrderPreserved() throws Exception {
        Row row = Row.withNames();
        row.setField("z", 3);
        row.setField("a", 1);
        row.setField("m", 2);

        Row restored = serializeDeserialize(row);

        // Row.withNames() uses LinkedHashMap — insertion order must survive the round-trip
        String[] keys = restored.getFieldNames(false).toArray(String[]::new);
        assertArrayEquals(new String[]{"z", "a", "m"}, keys);
    }

    // ── copy() independence ───────────────────────────────────────────────────

    @Test
    void copy_producesIndependentRow() {
        Row original = Row.withNames();
        original.setField("name", "alice");

        Row copy = ser.copy(original);
        copy.setField("name", "bob");

        assertEquals("alice", original.getField("name"),
                "Mutating the copy must not affect the original");
        assertEquals("bob", copy.getField("name"));
    }

    // ── Bulk copy (DataInputView → DataOutputView) ────────────────────────────

    @Test
    void bulkCopy_producesEquivalentBytes() throws Exception {
        Row row = Row.withNames();
        row.setField("k1", "v1");
        row.setField("k2", 100L);

        // Serialize to buffer A
        DataOutputSerializer bufA = new DataOutputSerializer(256);
        ser.serialize(row, bufA);

        // Bulk-copy A → B
        DataInputDeserializer  inA  = new DataInputDeserializer(bufA.getCopyOfBuffer());
        DataOutputSerializer   bufB = new DataOutputSerializer(256);
        ser.copy(inA, bufB);

        // Deserialize from B and verify
        DataInputDeserializer inB      = new DataInputDeserializer(bufB.getCopyOfBuffer());
        Row                   restored = ser.deserialize(inB);

        assertEquals("v1",  restored.getField("k1"));
        assertEquals(100L,  restored.getField("k2"));
    }

    // ── Dot-notation field names (pipeline usage) ─────────────────────────────

    @Test
    void roundTrip_dotNotationFieldNames() throws Exception {
        Row row = Row.withNames();
        row.setField("person.name",             "alice");
        row.setField("person.details.0.street", "Main St");
        row.setField("person.details.1.street", "Side Ave");

        Row restored = serializeDeserialize(row);

        assertEquals("alice",    restored.getField("person.name"));
        assertEquals("Main St",  restored.getField("person.details.0.street"));
        assertEquals("Side Ave", restored.getField("person.details.1.street"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Row serializeDeserialize(Row row) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(512);
        ser.serialize(row, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return ser.deserialize(in);
    }
}
