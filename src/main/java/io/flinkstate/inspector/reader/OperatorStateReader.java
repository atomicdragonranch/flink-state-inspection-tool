package io.flinkstate.inspector.reader;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.CompressibleFSDataInputStream;
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.RegisteredBroadcastStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredOperatorStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

public final class OperatorStateReader {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorStateReader.class);

    private OperatorStateReader() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static StateReadResult readOperatorState(
            String localCheckpointPath, String operatorUidOrHash,
            String stateName, String keyFilter, int limit) throws Exception {

        ClassLoader lenientCl = new LenientClassLoader(
            Thread.currentThread().getContextClassLoader());
        ClassLoader originalCl = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(lenientCl);

            File metadataFile = new File(localCheckpointPath, "_metadata");
            CheckpointMetadata metadata;
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(metadataFile)))) {
                metadata = Checkpoints.loadCheckpointMetadata(
                    dis, lenientCl, localCheckpointPath);
            }

            for (OperatorState opState : metadata.getOperatorStates()) {
                String uid = opState.getOperatorUid()
                    .orElse(opState.getOperatorID().toHexString());
                if (!uid.equals(operatorUidOrHash)) continue;

                List<Map<String, Object>> allEntries = new ArrayList<>();
                List<String> columns = null;
                boolean broadcastAlreadyRead = false;

                for (OperatorSubtaskState subtask : opState.getSubtaskStates().values()) {
                    for (OperatorStateHandle handle : subtask.getManagedOperatorState()) {
                        OperatorStateHandle.StateMetaInfo metaInfo =
                            handle.getStateNameToPartitionOffsets().get(stateName);
                        if (metaInfo == null) continue;

                        OperatorStateHandle.Mode mode = metaInfo.getDistributionMode();
                        if ((mode == OperatorStateHandle.Mode.BROADCAST
                                || mode == OperatorStateHandle.Mode.UNION)
                                && broadcastAlreadyRead) {
                            continue;
                        }

                        StreamStateHandle delegateHandle = handle.getDelegateStateHandle();
                        try (InputStream rawIn = GenericStateReader.openMetaHandle(
                                delegateHandle, localCheckpointPath)) {

                            FSDataInputStream fsIn = asFSDataInputStream(rawIn);
                            OperatorBackendSerializationProxy proxy =
                                new OperatorBackendSerializationProxy(lenientCl);
                            proxy.read(new DataInputViewStreamWrapper(fsIn));

                            StreamCompressionDecorator compressionDecorator =
                                isUsingCompression(proxy)
                                    ? SnappyStreamCompressionDecorator.INSTANCE
                                    : UncompressedStreamCompressionDecorator.INSTANCE;

                            StateSerializers serializers = findSerializers(
                                proxy, stateName);

                            CompressibleFSDataInputStream compressedIn =
                                new CompressibleFSDataInputStream(fsIn, compressionDecorator);

                            long[] offsets = metaInfo.getOffsets();

                            if (serializers.isBroadcast) {
                                List<Map<String, Object>> entries = readBroadcastEntries(
                                    compressedIn, offsets, serializers.keySerializer,
                                    serializers.valueSerializer, keyFilter,
                                    limit - allEntries.size());
                                allEntries.addAll(entries);
                                if (columns == null) columns = List.of("key", "value");
                                broadcastAlreadyRead = true;
                            } else {
                                List<Map<String, Object>> entries = readListEntries(
                                    compressedIn, offsets, serializers.valueSerializer,
                                    limit - allEntries.size());
                                allEntries.addAll(entries);
                                if (columns == null) columns = List.of("partition", "value");
                                if (mode == OperatorStateHandle.Mode.UNION) {
                                    broadcastAlreadyRead = true;
                                }
                            }
                        }

                        if (allEntries.size() >= limit) break;
                    }
                    if (allEntries.size() >= limit) break;
                }

                if (columns == null) columns = List.of("value");

                return new StateReadResult(operatorUidOrHash, allEntries, columns);
            }

            throw new IllegalStateException(
                "No operator found: " + operatorUidOrHash);
        } finally {
            Thread.currentThread().setContextClassLoader(originalCl);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static List<Map<String, Object>> readBroadcastEntries(
            FSDataInputStream in, long[] offsets,
            TypeSerializer keySerializer, TypeSerializer valueSerializer,
            String keyFilter, int limit) {

        List<Map<String, Object>> entries = new ArrayList<>();
        if (offsets.length == 0 || limit <= 0) return entries;

        try {
            in.seek(offsets[0]);
            DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(in);
            int count = div.readInt();

            for (int i = 0; i < count && entries.size() < limit; i++) {
                Object key;
                Object value;
                try {
                    key = keySerializer.deserialize(div);
                } catch (Exception e) {
                    LOG.warn("Failed to deserialize broadcast key at index {}: {}",
                        i, e.getMessage());
                    break;
                }
                try {
                    value = valueSerializer.deserialize(div);
                } catch (Exception e) {
                    value = GenericStateReader.rawBytesMap(new byte[0], e.getMessage());
                }

                String keyStr = String.valueOf(key);
                if (keyFilter != null && !keyFilter.isEmpty()
                        && !keyStr.contains(keyFilter)) {
                    continue;
                }

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("key", presentValue(key));
                entry.put("value", presentValue(value));
                entries.add(entry);
            }
        } catch (Exception e) {
            LOG.warn("Failed to read broadcast state: {}", e.getMessage());
        }
        return entries;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static List<Map<String, Object>> readListEntries(
            FSDataInputStream in, long[] offsets,
            TypeSerializer valueSerializer, int limit) {

        List<Map<String, Object>> entries = new ArrayList<>();
        if (limit <= 0) return entries;

        for (int i = 0; i < offsets.length && entries.size() < limit; i++) {
            try {
                in.seek(offsets[i]);
                DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(in);
                Object value = valueSerializer.deserialize(div);

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("partition", i);
                entry.put("value", presentValue(value));
                entries.add(entry);
            } catch (Exception e) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("partition", i);
                entry.put("value", GenericStateReader.rawBytesMap(new byte[0], e.getMessage()));
                entries.add(entry);
                LOG.warn("Failed to deserialize list state partition {}: {}",
                    i, e.getMessage());
            }
        }
        return entries;
    }

    static Object presentValue(Object value) {
        byte[] raw;
        if (value instanceof byte[]) {
            raw = (byte[]) value;
        } else if (value == null || value instanceof String || value instanceof Number
                || value instanceof Boolean || value instanceof Map
                || value instanceof List) {
            return value;
        } else {
            String className = value.getClass().getName();
            if (className.startsWith("io.flinkstate.inspector.reader.LenientClassLoader$")) {
                try {
                    java.lang.reflect.Field f = value.getClass().getDeclaredField("_serializedData");
                    f.setAccessible(true);
                    Object data = f.get(value);
                    if (data instanceof byte[]) {
                        raw = (byte[]) data;
                    } else {
                        return value;
                    }
                } catch (Exception e) {
                    return value;
                }
            } else {
                return value;
            }
        }

        Map<String, Object> structured = parseStructuredFields(raw);
        if (structured != null) {
            return structured;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("_raw", true);
        result.put("_bytes", raw.length);
        result.put("_hex", GenericStateReader.bytesToHex(raw, 256));
        if (raw.length <= 1024) {
            result.put("_base64", java.util.Base64.getEncoder().encodeToString(raw));
        }
        return result;
    }

    static Map<String, Object> parseStructuredFields(byte[] data) {
        if (data.length < 4) return null;

        Map<String, Object> fields = new LinkedHashMap<>();
        int pos = 0;
        int fieldNum = 0;

        try {
            while (pos < data.length) {
                int remaining = data.length - pos;

                if (remaining >= 2) {
                    int strLen = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
                    if (strLen > 0 && strLen <= remaining - 2 && isUtf8String(data, pos + 2, strLen)) {
                        String s = new String(data, pos + 2, strLen, StandardCharsets.UTF_8);
                        fields.put("field_" + fieldNum++, s);
                        pos += 2 + strLen;
                        continue;
                    }
                }

                if (remaining >= 8) {
                    long longVal = readLong(data, pos);
                    if (remaining >= 12) {
                        int nextStrLen = -1;
                        if (pos + 8 + 2 <= data.length) {
                            nextStrLen = ((data[pos + 8] & 0xFF) << 8) | (data[pos + 8 + 1] & 0xFF);
                        }
                        if (nextStrLen > 0 && pos + 8 + 2 + nextStrLen <= data.length
                                && isUtf8String(data, pos + 10, nextStrLen)) {
                            fields.put("field_" + fieldNum++, longVal);
                            pos += 8;
                            continue;
                        }
                    }

                    if (remaining >= 4) {
                        int intVal = readInt(data, pos);
                        if (intVal >= -1_000_000 && intVal <= 10_000_000 && remaining >= 8) {
                            fields.put("field_" + fieldNum++, intVal);
                            pos += 4;
                            continue;
                        }

                        if (longVal >= -1_000_000_000_000L && longVal <= 10_000_000_000_000L) {
                            fields.put("field_" + fieldNum++, longVal);
                            pos += 8;
                            continue;
                        }

                        fields.put("field_" + fieldNum++, intVal);
                        pos += 4;
                        continue;
                    }
                }

                if (remaining >= 4) {
                    fields.put("field_" + fieldNum++, readInt(data, pos));
                    pos += 4;
                    continue;
                }

                fields.put("field_" + fieldNum++,
                    GenericStateReader.bytesToHex(
                        java.util.Arrays.copyOfRange(data, pos, data.length), 256));
                break;
            }
        } catch (Exception e) {
            return null;
        }

        if (fields.size() < 2) return null;
        return fields;
    }

    private static boolean isUtf8String(byte[] data, int offset, int len) {
        if (offset + len > data.length) return false;
        int printable = 0;
        for (int i = 0; i < len; i++) {
            int b = data[offset + i] & 0xFF;
            if (b >= 0x20 && b <= 0x7E) {
                printable++;
            } else if (b == 0x09 || b == 0x0A || b == 0x0D) {
                // tabs and newlines are ok
            } else if (b >= 0xC0 && b <= 0xF7) {
                // multi-byte UTF-8 lead byte, ok
            } else if (b >= 0x80 && b <= 0xBF) {
                // UTF-8 continuation byte, ok
            } else {
                return false;
            }
        }
        return printable > len / 2;
    }

    private static int readInt(byte[] data, int pos) {
        return ((data[pos] & 0xFF) << 24)
             | ((data[pos + 1] & 0xFF) << 16)
             | ((data[pos + 2] & 0xFF) << 8)
             | (data[pos + 3] & 0xFF);
    }

    private static long readLong(byte[] data, int pos) {
        return ((long)(data[pos] & 0xFF) << 56)
             | ((long)(data[pos + 1] & 0xFF) << 48)
             | ((long)(data[pos + 2] & 0xFF) << 40)
             | ((long)(data[pos + 3] & 0xFF) << 32)
             | ((long)(data[pos + 4] & 0xFF) << 24)
             | ((long)(data[pos + 5] & 0xFF) << 16)
             | ((long)(data[pos + 6] & 0xFF) << 8)
             | (long)(data[pos + 7] & 0xFF);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StateSerializers findSerializers(
            OperatorBackendSerializationProxy proxy, String stateName) {

        for (StateMetaInfoSnapshot snapshot : proxy.getBroadcastStateMetaInfoSnapshots()) {
            if (snapshot.getName().equals(stateName)) {
                RegisteredBroadcastStateBackendMetaInfo meta =
                    new RegisteredBroadcastStateBackendMetaInfo(snapshot);
                return new StateSerializers(
                    true, meta.getKeySerializer(), meta.getValueSerializer());
            }
        }

        for (StateMetaInfoSnapshot snapshot : proxy.getOperatorStateMetaInfoSnapshots()) {
            if (snapshot.getName().equals(stateName)) {
                RegisteredOperatorStateBackendMetaInfo meta =
                    new RegisteredOperatorStateBackendMetaInfo(snapshot);
                return new StateSerializers(
                    false, null, meta.getPartitionStateSerializer());
            }
        }

        return new StateSerializers(false, null, null);
    }

    private static FSDataInputStream asFSDataInputStream(InputStream in) {
        if (in instanceof FSDataInputStream) {
            return (FSDataInputStream) in;
        }
        return new InputStreamAdapter(in);
    }

    private static boolean isUsingCompression(OperatorBackendSerializationProxy proxy) {
        try {
            java.lang.reflect.Method m =
                OperatorBackendSerializationProxy.class.getDeclaredMethod("isUsingStateCompression");
            m.setAccessible(true);
            return (boolean) m.invoke(proxy);
        } catch (Exception e) {
            LOG.debug("Could not check compression flag, assuming uncompressed: {}",
                e.getMessage());
            return false;
        }
    }

    private static class StateSerializers {
        final boolean isBroadcast;
        @SuppressWarnings("rawtypes")
        final TypeSerializer keySerializer;
        @SuppressWarnings("rawtypes")
        final TypeSerializer valueSerializer;

        @SuppressWarnings("rawtypes")
        StateSerializers(boolean isBroadcast,
                         TypeSerializer keySerializer, TypeSerializer valueSerializer) {
            this.isBroadcast = isBroadcast;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }
    }

    static class InputStreamAdapter extends FSDataInputStream {
        private final InputStream delegate;
        private long pos;

        InputStreamAdapter(InputStream delegate) {
            this.delegate = delegate;
            this.pos = 0;
        }

        @Override
        public void seek(long desired) throws java.io.IOException {
            if (desired > pos) {
                long toSkip = desired - pos;
                long skipped = 0;
                while (skipped < toSkip) {
                    long n = delegate.skip(toSkip - skipped);
                    if (n <= 0) {
                        int b = delegate.read();
                        if (b < 0) break;
                        skipped++;
                    } else {
                        skipped += n;
                    }
                }
                pos += skipped;
            }
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read() throws java.io.IOException {
            int b = delegate.read();
            if (b >= 0) pos++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws java.io.IOException {
            int n = delegate.read(b, off, len);
            if (n > 0) pos += n;
            return n;
        }

        @Override
        public void close() throws java.io.IOException {
            delegate.close();
        }
    }
}
