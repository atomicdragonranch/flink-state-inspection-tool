package io.flinkstate.inspector.reader;

import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GenericStateReaderTest {

    // --- deserializeValue tests ---

    @Test
    void deserializeValueHandlesString() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(StringSerializer.INSTANCE, "hello");
        GenericStateReader.StateDescriptorEntry sde = valueEntry("test", StringSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo("hello");
    }

    @Test
    void deserializeValueHandlesLong() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(LongSerializer.INSTANCE, 42L);
        GenericStateReader.StateDescriptorEntry sde = valueEntry("count", LongSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo(42L);
    }

    @Test
    void deserializeValueHandlesDouble() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(DoubleSerializer.INSTANCE, 3.14);
        GenericStateReader.StateDescriptorEntry sde = valueEntry("avg", DoubleSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo(3.14);
    }

    @SuppressWarnings("unchecked")
    @Test
    void deserializeValueHandlesList() throws Exception {
        // Arrange - RocksDB list format: elements separated by 0x2c merge delimiter
        DataOutputSerializer out = new DataOutputSerializer(64);
        StringSerializer.INSTANCE.serialize("a", out);
        out.write(0x2c);
        StringSerializer.INSTANCE.serialize("b", out);
        out.write(0x2c);
        StringSerializer.INSTANCE.serialize("c", out);
        byte[] serialized = out.getCopyOfBuffer();
        GenericStateReader.StateDescriptorEntry sde = listEntry("items", StringSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isInstanceOf(List.class);
        assertThat((List<Object>) result).containsExactly("a", "b", "c");
    }

    @SuppressWarnings("unchecked")
    @Test
    void deserializeValueHandlesMap() throws Exception {
        // Arrange
        DataOutputSerializer out = new DataOutputSerializer(64);
        StringSerializer.INSTANCE.serialize("key1", out);
        IntSerializer.INSTANCE.serialize(100, out);
        StringSerializer.INSTANCE.serialize("key2", out);
        IntSerializer.INSTANCE.serialize(200, out);
        byte[] serialized = out.getCopyOfBuffer();
        GenericStateReader.StateDescriptorEntry sde = mapEntry(
            "props", IntSerializer.INSTANCE, StringSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isInstanceOf(Map.class);
        Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map).containsEntry("key1", 100);
        assertThat(map).containsEntry("key2", 200);
    }

    @SuppressWarnings("unchecked")
    @Test
    void deserializeValueFallsBackToRawBytesOnFailure() {
        // Arrange
        byte[] garbage = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        GenericStateReader.StateDescriptorEntry sde = valueEntry("bad", LongSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, garbage);

        // Assert
        assertThat(result).isInstanceOf(Map.class);
        Map<String, Object> raw = (Map<String, Object>) result;
        assertThat(raw.get("_raw")).isEqualTo(true);
        assertThat(raw.get("_bytes")).isEqualTo(4);
        assertThat((String) raw.get("_hex")).isEqualTo("ffffffff");
        assertThat(raw).containsKey("_base64");
        assertThat(raw).containsKey("_reason");
    }

    @SuppressWarnings("unchecked")
    @Test
    void deserializeValueFallsBackForUnsupportedType() {
        // Arrange
        byte[] data = {0x0A, 0x0B};
        GenericStateReader.StateDescriptorEntry sde =
            new GenericStateReader.StateDescriptorEntry("x", "FOLDING", StringSerializer.INSTANCE, null, null, null);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, data);

        // Assert
        assertThat(result).isInstanceOf(Map.class);
        Map<String, Object> raw = (Map<String, Object>) result;
        assertThat(raw.get("_raw")).isEqualTo(true);
        assertThat(raw.get("_reason")).isEqualTo("FOLDING");
    }

    @Test
    void deserializeValueHandlesReducingState() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(LongSerializer.INSTANCE, 99L);
        GenericStateReader.StateDescriptorEntry sde = reducingEntry("runningTotal", LongSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo(99L);
    }

    @Test
    void deserializeValueHandlesReducingStateString() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(StringSerializer.INSTANCE, "reduced-value");
        GenericStateReader.StateDescriptorEntry sde = reducingEntry("concat", StringSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo("reduced-value");
    }

    @Test
    void deserializeValueHandlesAggregatingState() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(DoubleSerializer.INSTANCE, 42.5);
        GenericStateReader.StateDescriptorEntry sde = aggregatingEntry("average", DoubleSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo(42.5);
    }

    @Test
    void deserializeValueHandlesAggregatingStateLong() throws Exception {
        // Arrange
        byte[] serialized = serializeValue(LongSerializer.INSTANCE, 1000L);
        GenericStateReader.StateDescriptorEntry sde = aggregatingEntry("sum", LongSerializer.INSTANCE);

        // Act
        Object result = GenericStateReader.deserializeValue(sde, serialized);

        // Assert
        assertThat(result).isEqualTo(1000L);
    }

    // --- rawBytesMap tests ---

    @Test
    void rawBytesMapIncludesAllFields() {
        // Arrange
        byte[] data = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};

        // Act
        Map<String, Object> result = GenericStateReader.rawBytesMap(data, "test reason");

        // Assert
        assertThat(result.get("_raw")).isEqualTo(true);
        assertThat(result.get("_reason")).isEqualTo("test reason");
        assertThat(result.get("_bytes")).isEqualTo(4);
        assertThat(result.get("_hex")).isEqualTo("cafebabe");
        assertThat(result).containsKey("_base64");
    }

    @Test
    void rawBytesMapOmitsBase64ForLargeData() {
        // Arrange
        byte[] data = new byte[2048];

        // Act
        Map<String, Object> result = GenericStateReader.rawBytesMap(data, "big");

        // Assert
        assertThat(result).doesNotContainKey("_base64");
        assertThat(result.get("_bytes")).isEqualTo(2048);
    }

    // --- bytesToHex tests ---

    @Test
    void bytesToHexConvertsCorrectly() {
        // Arrange
        byte[] data = {0x00, 0x0F, (byte) 0xFF, 0x7F};

        // Act
        String hex = GenericStateReader.bytesToHex(data, 256);

        // Assert
        assertThat(hex).isEqualTo("000fff7f");
    }

    @Test
    void bytesToHexTruncatesWithEllipsis() {
        // Arrange
        byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};

        // Act
        String hex = GenericStateReader.bytesToHex(data, 3);

        // Assert
        assertThat(hex).isEqualTo("010203...");
    }

    @Test
    void bytesToHexHandlesEmptyArray() {
        // Arrange
        byte[] data = {};

        // Act
        String hex = GenericStateReader.bytesToHex(data, 256);

        // Assert
        assertThat(hex).isEmpty();
    }

    // --- isFrocksdbFormatError tests ---

    @Test
    void isFrocksdbFormatErrorDetectsMagicNumber() {
        // Arrange
        RocksDBException ex = new RocksDBException(
            "Corruption: Bad table magic number: expected 9863518390377041911, got 4994500903858853830 in /tmp/sst/000042.sst");

        // Act
        boolean result = GenericStateReader.isFrocksdbFormatError(ex);

        // Assert
        assertThat(result).isTrue();
    }

    @Test
    void isFrocksdbFormatErrorDetectsNotAnSstable() {
        // Arrange
        RocksDBException ex = new RocksDBException(
            "Corruption: not an sstable (bad magic number)");

        // Act
        boolean result = GenericStateReader.isFrocksdbFormatError(ex);

        // Assert
        assertThat(result).isTrue();
    }

    @Test
    void isFrocksdbFormatErrorDetectsSstCorruption() {
        // Arrange
        RocksDBException ex = new RocksDBException(
            "Corruption in sstable file data");

        // Act
        boolean result = GenericStateReader.isFrocksdbFormatError(ex);

        // Assert
        assertThat(result).isTrue();
    }

    @Test
    void isFrocksdbFormatErrorReturnsFalseForUnrelatedError() {
        // Arrange
        RocksDBException ex = new RocksDBException("File not found: /tmp/missing.sst");

        // Act
        boolean result = GenericStateReader.isFrocksdbFormatError(ex);

        // Assert
        assertThat(result).isFalse();
    }

    @Test
    void isFrocksdbFormatErrorHandlesNullMessage() {
        // Arrange
        RocksDBException ex = new RocksDBException((String) null);

        // Act
        boolean result = GenericStateReader.isFrocksdbFormatError(ex);

        // Assert
        assertThat(result).isFalse();
    }

    // --- helpers ---

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] serializeValue(
            org.apache.flink.api.common.typeutils.TypeSerializer serializer,
            Object value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    @SuppressWarnings("rawtypes")
    private static GenericStateReader.StateDescriptorEntry valueEntry(
            String name, org.apache.flink.api.common.typeutils.TypeSerializer serializer) {
        return new GenericStateReader.StateDescriptorEntry(name, "VALUE", serializer, null, null, null);
    }

    @SuppressWarnings("rawtypes")
    private static GenericStateReader.StateDescriptorEntry listEntry(
            String name, org.apache.flink.api.common.typeutils.TypeSerializer serializer) {
        return new GenericStateReader.StateDescriptorEntry(name, "LIST", serializer, null, null, serializer);
    }

    @SuppressWarnings("rawtypes")
    private static GenericStateReader.StateDescriptorEntry mapEntry(
            String name,
            org.apache.flink.api.common.typeutils.TypeSerializer valueSerializer,
            org.apache.flink.api.common.typeutils.TypeSerializer mapKeySerializer) {
        return new GenericStateReader.StateDescriptorEntry(name, "MAP", valueSerializer, mapKeySerializer, null, null);
    }

    @SuppressWarnings("rawtypes")
    private static GenericStateReader.StateDescriptorEntry reducingEntry(
            String name, org.apache.flink.api.common.typeutils.TypeSerializer serializer) {
        return new GenericStateReader.StateDescriptorEntry(name, "REDUCING", serializer, null, null, null);
    }

    @SuppressWarnings("rawtypes")
    private static GenericStateReader.StateDescriptorEntry aggregatingEntry(
            String name, org.apache.flink.api.common.typeutils.TypeSerializer serializer) {
        return new GenericStateReader.StateDescriptorEntry(name, "AGGREGATING", serializer, null, null, null);
    }
}
