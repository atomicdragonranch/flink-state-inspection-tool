package io.flinkstate.inspector.reader;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorStateReaderTest {

    // --- readBroadcastEntries tests ---

    @SuppressWarnings("unchecked")
    @Test
    void readBroadcastEntriesDeserializesKeyValuePairs() throws Exception {
        // Arrange
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeInt(2);
        StringSerializer.INSTANCE.serialize("configKey1", out);
        IntSerializer.INSTANCE.serialize(100, out);
        StringSerializer.INSTANCE.serialize("configKey2", out);
        IntSerializer.INSTANCE.serialize(200, out);
        byte[] data = out.getCopyOfBuffer();

        FSDataInputStream in = toFSDataInputStream(data);
        long[] offsets = {0};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readBroadcastEntries(
            in, offsets, StringSerializer.INSTANCE, IntSerializer.INSTANCE,
            null, 100);

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).get("key")).isEqualTo("configKey1");
        assertThat(entries.get(0).get("value")).isEqualTo(100);
        assertThat(entries.get(1).get("key")).isEqualTo("configKey2");
        assertThat(entries.get(1).get("value")).isEqualTo(200);
    }

    @Test
    void readBroadcastEntriesRespectsLimit() throws Exception {
        // Arrange
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeInt(3);
        StringSerializer.INSTANCE.serialize("a", out);
        IntSerializer.INSTANCE.serialize(1, out);
        StringSerializer.INSTANCE.serialize("b", out);
        IntSerializer.INSTANCE.serialize(2, out);
        StringSerializer.INSTANCE.serialize("c", out);
        IntSerializer.INSTANCE.serialize(3, out);
        byte[] data = out.getCopyOfBuffer();

        FSDataInputStream in = toFSDataInputStream(data);
        long[] offsets = {0};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readBroadcastEntries(
            in, offsets, StringSerializer.INSTANCE, IntSerializer.INSTANCE,
            null, 2);

        // Assert
        assertThat(entries).hasSize(2);
    }

    @Test
    void readBroadcastEntriesFiltersbyKey() throws Exception {
        // Arrange
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeInt(3);
        StringSerializer.INSTANCE.serialize("alpha", out);
        IntSerializer.INSTANCE.serialize(1, out);
        StringSerializer.INSTANCE.serialize("beta", out);
        IntSerializer.INSTANCE.serialize(2, out);
        StringSerializer.INSTANCE.serialize("alphabeta", out);
        IntSerializer.INSTANCE.serialize(3, out);
        byte[] data = out.getCopyOfBuffer();

        FSDataInputStream in = toFSDataInputStream(data);
        long[] offsets = {0};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readBroadcastEntries(
            in, offsets, StringSerializer.INSTANCE, IntSerializer.INSTANCE,
            "alpha", 100);

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).get("key")).isEqualTo("alpha");
        assertThat(entries.get(1).get("key")).isEqualTo("alphabeta");
    }

    @Test
    void readBroadcastEntriesHandlesEmptyOffsets() {
        // Arrange
        FSDataInputStream in = toFSDataInputStream(new byte[0]);
        long[] offsets = {};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readBroadcastEntries(
            in, offsets, StringSerializer.INSTANCE, IntSerializer.INSTANCE,
            null, 100);

        // Assert
        assertThat(entries).isEmpty();
    }

    // --- readListEntries tests ---

    @Test
    void readListEntriesDeserializesPartitions() throws Exception {
        // Arrange
        DataOutputSerializer out0 = new DataOutputSerializer(32);
        StringSerializer.INSTANCE.serialize("partition-zero", out0);
        byte[] p0 = out0.getCopyOfBuffer();

        DataOutputSerializer out1 = new DataOutputSerializer(32);
        StringSerializer.INSTANCE.serialize("partition-one", out1);
        byte[] p1 = out1.getCopyOfBuffer();

        byte[] combined = new byte[p0.length + p1.length];
        System.arraycopy(p0, 0, combined, 0, p0.length);
        System.arraycopy(p1, 0, combined, p0.length, p1.length);

        FSDataInputStream in = toFSDataInputStream(combined);
        long[] offsets = {0, p0.length};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readListEntries(
            in, offsets, StringSerializer.INSTANCE, 100);

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).get("partition")).isEqualTo(0);
        assertThat(entries.get(0).get("value")).isEqualTo("partition-zero");
        assertThat(entries.get(1).get("partition")).isEqualTo(1);
        assertThat(entries.get(1).get("value")).isEqualTo("partition-one");
    }

    @Test
    void readListEntriesRespectsLimit() throws Exception {
        // Arrange
        DataOutputSerializer out = new DataOutputSerializer(64);
        LongSerializer.INSTANCE.serialize(100L, out);
        LongSerializer.INSTANCE.serialize(200L, out);
        LongSerializer.INSTANCE.serialize(300L, out);
        byte[] data = out.getCopyOfBuffer();

        FSDataInputStream in = toFSDataInputStream(data);
        long[] offsets = {0, 8, 16};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readListEntries(
            in, offsets, LongSerializer.INSTANCE, 2);

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).get("value")).isEqualTo(100L);
        assertThat(entries.get(1).get("value")).isEqualTo(200L);
    }

    @SuppressWarnings("unchecked")
    @Test
    void readListEntriesFallsBackToRawBytesOnError() {
        // Arrange
        byte[] garbage = {(byte) 0xFF, (byte) 0xFE};
        FSDataInputStream in = toFSDataInputStream(garbage);
        long[] offsets = {0};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readListEntries(
            in, offsets, LongSerializer.INSTANCE, 100);

        // Assert
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).get("partition")).isEqualTo(0);
        Object value = entries.get(0).get("value");
        assertThat(value).isInstanceOf(Map.class);
        Map<String, Object> raw = (Map<String, Object>) value;
        assertThat(raw.get("_raw")).isEqualTo(true);
    }

    @Test
    void readListEntriesHandlesEmptyOffsets() {
        // Arrange
        FSDataInputStream in = toFSDataInputStream(new byte[0]);
        long[] offsets = {};

        // Act
        List<Map<String, Object>> entries = OperatorStateReader.readListEntries(
            in, offsets, StringSerializer.INSTANCE, 100);

        // Assert
        assertThat(entries).isEmpty();
    }

    // --- presentValue tests ---

    @SuppressWarnings("unchecked")
    @Test
    void presentValueConvertsbyteArrayToRawMap() {
        // Arrange
        byte[] data = {0x00, 0x05, 'h', 'e', 'l', 'l', 'o', 0x00};

        // Act
        Object result = OperatorStateReader.presentValue(data);

        // Assert
        assertThat(result).isInstanceOf(Map.class);
        Map<String, Object> raw = (Map<String, Object>) result;
        assertThat(raw.get("_raw")).isEqualTo(true);
        assertThat(raw.get("_bytes")).isEqualTo(8);
        assertThat(raw).containsKey("_hex");
        assertThat(raw).containsKey("_base64");
        assertThat(raw).containsKey("_strings");
        List<String> strings = (List<String>) raw.get("_strings");
        assertThat(strings).contains("hello");
    }

    @Test
    void presentValuePassesThroughPrimitives() {
        // Arrange / Act / Assert
        assertThat(OperatorStateReader.presentValue("hello")).isEqualTo("hello");
        assertThat(OperatorStateReader.presentValue(42)).isEqualTo(42);
        assertThat(OperatorStateReader.presentValue(true)).isEqualTo(true);
        assertThat(OperatorStateReader.presentValue(null)).isNull();
    }

    // --- extractStrings tests ---

    @Test
    void extractStringsFindsEmbeddedText() {
        // Arrange
        byte[] data = {0x00, 0x00, 's', 't', 'r', 'e', 'a', 'm', '-',
            'e', 'v', 'e', 'n', 't', 's', 0x00, 0x00};

        // Act
        List<String> strings = OperatorStateReader.extractStrings(data);

        // Assert
        assertThat(strings).containsExactly("stream-events");
    }

    @Test
    void extractStringsIgnoresShortRuns() {
        // Arrange
        byte[] data = {0x00, 'a', 'b', 0x00};

        // Act
        List<String> strings = OperatorStateReader.extractStrings(data);

        // Assert
        assertThat(strings).isEmpty();
    }

    // --- helpers ---

    private static FSDataInputStream toFSDataInputStream(byte[] data) {
        return new OperatorStateReader.InputStreamAdapter(new ByteArrayInputStream(data));
    }
}
