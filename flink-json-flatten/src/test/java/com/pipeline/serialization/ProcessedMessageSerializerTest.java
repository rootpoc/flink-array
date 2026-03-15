package com.pipeline.serialization;

import com.pipeline.common.ProcessedMessage;
import com.pipeline.common.typeinfo.ProcessedMessageSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Direct tests for {@link ProcessedMessageSerializer}. */
class ProcessedMessageSerializerTest {

    private final ProcessedMessageSerializer ser = new ProcessedMessageSerializer();

    @Test
    void roundTrip_withKeyHeadersAndPayload_preservesAllFields() throws Exception {
        Row row = Row.withNames();
        row.setField("name", "Alice");
        row.setField("age", 42L);

        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put("h1", new byte[]{1, 2});
        headers.put("h2", new byte[]{3});

        ProcessedMessage original = new ProcessedMessage(
                new byte[]{9, 8},
                new byte[]{7, 6, 5},
                headers,
                row);

        ProcessedMessage restored = roundTrip(original);

        assertArrayEquals(original.getKafkaKey(), restored.getKafkaKey());
        assertArrayEquals(original.getOriginalBytes(), restored.getOriginalBytes());
        assertEquals(2, restored.getHeaders().size());
        assertArrayEquals(new byte[]{1, 2}, restored.getHeaders().get("h1"));
        assertArrayEquals(new byte[]{3}, restored.getHeaders().get("h2"));
        assertNotNull(restored.getPayload());
        assertEquals("Alice", restored.getPayload().getField("name"));
        assertEquals(42L, restored.getPayload().getField("age"));
    }

    @Test
    void roundTrip_nullKeyEmptyOriginalBytesNullPayload_supported() throws Exception {
        ProcessedMessage original = new ProcessedMessage(
                null,
                new byte[0],
                Map.of(),
                null);

        ProcessedMessage restored = roundTrip(original);

        assertNull(restored.getKafkaKey());
        assertArrayEquals(new byte[0], restored.getOriginalBytes());
        assertTrue(restored.getHeaders().isEmpty());
        assertNull(restored.getPayload());
    }

    @Test
    void copy_binaryCopy_matchesDeserializeRoundTrip() throws Exception {
        Row row = Row.withNames();
        row.setField("person.name", "Bob");
        row.setField("person.age", 33L);

        ProcessedMessage original = new ProcessedMessage(
                new byte[]{1},
                new byte[]{2, 3},
                Map.of("trace", new byte[]{4, 5, 6}),
                row);

        DataOutputSerializer sourceOut = new DataOutputSerializer(128);
        ser.serialize(original, sourceOut);

        DataInputDeserializer sourceIn = new DataInputDeserializer(sourceOut.getSharedBuffer());
        DataOutputSerializer copiedOut = new DataOutputSerializer(128);
        ser.copy(sourceIn, copiedOut);

        ProcessedMessage copied = ser.deserialize(new DataInputDeserializer(copiedOut.getSharedBuffer()));

        assertArrayEquals(original.getKafkaKey(), copied.getKafkaKey());
        assertArrayEquals(original.getOriginalBytes(), copied.getOriginalBytes());
        assertArrayEquals(original.getHeaders().get("trace"), copied.getHeaders().get("trace"));
        assertEquals("Bob", copied.getPayload().getField("person.name"));
        assertEquals(33L, copied.getPayload().getField("person.age"));
    }

    private ProcessedMessage roundTrip(ProcessedMessage original) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(128);
        ser.serialize(original, out);
        return ser.deserialize(new DataInputDeserializer(out.getSharedBuffer()));
    }
}

