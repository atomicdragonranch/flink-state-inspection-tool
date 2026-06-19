package io.flinkstate.inspector.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class GenericStateReader {

    private static final Logger LOG = LoggerFactory.getLogger(GenericStateReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private GenericStateReader() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static StateReadResult readKeyedState(
            String localCheckpointPath, String operatorUidOrHash,
            String keyFilter, boolean keysOnly, int limit) throws Exception {

        ClassLoader lenientCl = new LenientClassLoader(
            Thread.currentThread().getContextClassLoader());
        ClassLoader originalCl = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(lenientCl);

            OperatorInfo opInfo = discoverOperatorInfo(localCheckpointPath, operatorUidOrHash);

            Map<String, StateDescriptorEntry> stateByName = new HashMap<>();
            List<StateDescriptorEntry> sdEntries = new ArrayList<>();
            for (StateMetaInfoSnapshot info : opInfo.stateInfos) {
                if (info.getBackendStateType() != StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                    continue;
                }
                String stateType = info.getOption(
                    StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE);
                TypeSerializer valueSerializer = info
                    .getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER)
                    .restoreSerializer();

                TypeSerializer mapKeySerializer = null;
                if ("MAP".equals(stateType)) {
                    mapKeySerializer = info
                        .getTypeSerializerSnapshot(
                            StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER)
                        .restoreSerializer();
                }

                StateDescriptorEntry sde = new StateDescriptorEntry(
                    info.getName(), stateType, valueSerializer, mapKeySerializer);
                sdEntries.add(sde);
                stateByName.put(info.getName(), sde);
            }

            TypeSerializer keySerializer = opInfo.keySerializerSnapshot.restoreSerializer();
            int keyGroupPrefixBytes = CompositeKeySerializationUtils
                .computeRequiredBytesInKeyGroupPrefix(opInfo.maxParallelism);
            boolean ambiguousKeyPossible = CompositeKeySerializationUtils
                .isAmbiguousKeyPossible(keySerializer, keySerializer);

            List<String> sstFiles = resolveSstFiles(opInfo.stateHandle, localCheckpointPath);

            Map<String, Map<String, Object>> keyEntries = new LinkedHashMap<>();

            for (String sstFile : sstFiles) {
                try (Options opts = new Options();
                     SstFileReader reader = new SstFileReader(opts)) {
                    reader.open(sstFile);
                    byte[] cfNameBytes = reader.getTableProperties().getColumnFamilyName();
                    String cfName = new String(cfNameBytes, StandardCharsets.UTF_8);

                    StateDescriptorEntry sde = stateByName.get(cfName);
                    if (sde == null) continue;

                    try (ReadOptions readOpts = new ReadOptions();
                         SstFileReaderIterator iter = reader.newIterator(readOpts)) {
                        iter.seekToFirst();
                        while (iter.isValid()) {
                            byte[] rawKey = iter.key();
                            byte[] rawValue = iter.value();

                            DataInputDeserializer keyInput = new DataInputDeserializer(
                                rawKey, keyGroupPrefixBytes,
                                rawKey.length - keyGroupPrefixBytes);
                            Object key = CompositeKeySerializationUtils
                                .readKey(keySerializer, keyInput, ambiguousKeyPossible);
                            String keyStr = String.valueOf(key);

                            if (keyFilter != null && !keyFilter.isEmpty()
                                    && !keyStr.contains(keyFilter)) {
                                iter.next();
                                continue;
                            }

                            Map<String, Object> entry = keyEntries.computeIfAbsent(
                                keyStr, k -> {
                                    Map<String, Object> e = new LinkedHashMap<>();
                                    e.put("key", key);
                                    return e;
                                });

                            Object value = deserializeValue(sde, rawValue);
                            entry.put(sde.name, value);

                            iter.next();
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to read SST file {}: {}", sstFile, e.getMessage());
                }
            }

            List<Map<String, Object>> results = new ArrayList<>();
            for (Map<String, Object> entry : keyEntries.values()) {
                if (results.size() >= limit) break;
                if (keysOnly) {
                    Map<String, Object> keyOnly = new LinkedHashMap<>();
                    keyOnly.put("key", entry.get("key"));
                    results.add(keyOnly);
                } else {
                    results.add(entry);
                }
            }

            List<String> columns = new ArrayList<>();
            columns.add("key");
            if (!keysOnly) {
                for (StateDescriptorEntry e : sdEntries) {
                    columns.add(e.name);
                }
            }

            return new StateReadResult(operatorUidOrHash, results, columns);
        } finally {
            Thread.currentThread().setContextClassLoader(originalCl);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Object deserializeValue(StateDescriptorEntry sde, byte[] rawValue) {
        try {
            DataInputDeserializer valueInput = new DataInputDeserializer(rawValue);
            switch (sde.stateType) {
                case "VALUE":
                    return sde.valueSerializer.deserialize(valueInput);
                case "LIST": {
                    List<Object> items = new ArrayList<>();
                    while (valueInput.available() > 0) {
                        items.add(sde.valueSerializer.deserialize(valueInput));
                    }
                    return items;
                }
                case "MAP": {
                    Map<String, Object> map = new LinkedHashMap<>();
                    while (valueInput.available() > 0) {
                        Object mk = sde.mapKeySerializer.deserialize(valueInput);
                        Object mv = sde.valueSerializer.deserialize(valueInput);
                        map.put(String.valueOf(mk), mv);
                    }
                    return map;
                }
                default:
                    return rawBytesMap(rawValue, sde.stateType);
            }
        } catch (Exception e) {
            return rawBytesMap(rawValue, e.getMessage());
        }
    }

    static Map<String, Object> rawBytesMap(byte[] rawValue, String reason) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("_raw", true);
        result.put("_reason", reason);
        result.put("_bytes", rawValue.length);
        result.put("_hex", bytesToHex(rawValue, 256));
        if (rawValue.length <= 1024) {
            result.put("_base64", java.util.Base64.getEncoder().encodeToString(rawValue));
        }
        return result;
    }

    static String bytesToHex(byte[] bytes, int maxBytes) {
        int len = Math.min(bytes.length, maxBytes);
        StringBuilder sb = new StringBuilder(len * 2 + (len > maxBytes ? 3 : 0));
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02x", bytes[i] & 0xFF));
        }
        if (bytes.length > maxBytes) sb.append("...");
        return sb.toString();
    }

    private static List<String> resolveSstFiles(
            IncrementalRemoteKeyedStateHandle handle,
            String localCheckpointPath) throws Exception {
        List<String> paths = new ArrayList<>();
        for (HandleAndLocalPath hp : handle.getSharedState()) {
            if (!hp.getLocalPath().endsWith(".sst")) continue;
            String resolved = resolveHandlePath(hp.getHandle(), localCheckpointPath);
            if (resolved != null) paths.add(resolved);
        }
        for (HandleAndLocalPath hp : handle.getPrivateState()) {
            if (!hp.getLocalPath().endsWith(".sst")) continue;
            String resolved = resolveHandlePath(hp.getHandle(), localCheckpointPath);
            if (resolved != null) paths.add(resolved);
        }
        return paths;
    }

    private static String resolveHandlePath(
            StreamStateHandle handle, String localCheckpointPath) throws Exception {
        if (handle instanceof RelativeFileStateHandle) {
            String fileName = ((RelativeFileStateHandle) handle).getRelativePath();
            File f = new File(localCheckpointPath, fileName);
            if (f.exists()) return f.getAbsolutePath();
        } else if (handle instanceof FileStateHandle) {
            String fileName = ((FileStateHandle) handle).getFilePath().getName();
            File f = new File(localCheckpointPath, fileName);
            if (f.exists()) return f.getAbsolutePath();
        }
        try (InputStream in = handle.openInputStream()) {
            File tempFile = File.createTempFile("sst-", ".sst",
                new File(localCheckpointPath));
            tempFile.deleteOnExit();
            try (FileOutputStream out = new FileOutputStream(tempFile)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            }
            return tempFile.getAbsolutePath();
        } catch (Exception e) {
            LOG.warn("Failed to extract state handle: {}", e.getMessage());
            return null;
        }
    }

    static OperatorInfo discoverOperatorInfo(
            String localCheckpointPath, String operatorUidOrHash) throws Exception {

        ClassLoader lenientCl = new LenientClassLoader(
            Thread.currentThread().getContextClassLoader());

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

            for (OperatorSubtaskState subtask : opState.getSubtaskStates().values()) {
                for (KeyedStateHandle handle : subtask.getManagedKeyedState()) {
                    if (handle instanceof IncrementalRemoteKeyedStateHandle) {
                        IncrementalRemoteKeyedStateHandle inc =
                            (IncrementalRemoteKeyedStateHandle) handle;
                        StreamStateHandle metaHandle = inc.getMetaDataStateHandle();

                        try (InputStream in = openMetaHandle(metaHandle, localCheckpointPath)) {
                            KeyedBackendSerializationProxy<?> proxy =
                                new KeyedBackendSerializationProxy<>(lenientCl);
                            proxy.read(new DataInputViewStreamWrapper(in));
                            return new OperatorInfo(
                                proxy.getKeySerializerSnapshot(),
                                proxy.getStateMetaInfoSnapshots(),
                                opState.getMaxParallelism(),
                                inc);
                        }
                    }
                }
            }
        }
        throw new IllegalStateException(
            "No keyed state found for operator: " + operatorUidOrHash);
    }

    private static InputStream openMetaHandle(
            StreamStateHandle handle, String localCheckpointPath) throws Exception {
        if (handle instanceof RelativeFileStateHandle) {
            String relativePath = ((RelativeFileStateHandle) handle).getRelativePath();
            return new FileInputStream(new File(localCheckpointPath, relativePath));
        }
        if (handle instanceof FileStateHandle) {
            String fileName = ((FileStateHandle) handle).getFilePath().getName();
            File localFile = new File(localCheckpointPath, fileName);
            if (localFile.exists()) {
                return new FileInputStream(localFile);
            }
        }
        return handle.openInputStream();
    }

    static class OperatorInfo {
        final TypeSerializerSnapshot<?> keySerializerSnapshot;
        final List<StateMetaInfoSnapshot> stateInfos;
        final int maxParallelism;
        final IncrementalRemoteKeyedStateHandle stateHandle;

        OperatorInfo(TypeSerializerSnapshot<?> keySerializerSnapshot,
                     List<StateMetaInfoSnapshot> stateInfos,
                     int maxParallelism,
                     IncrementalRemoteKeyedStateHandle stateHandle) {
            this.keySerializerSnapshot = keySerializerSnapshot;
            this.stateInfos = stateInfos;
            this.maxParallelism = maxParallelism;
            this.stateHandle = stateHandle;
        }
    }

    static class StateDescriptorEntry implements Serializable {
        private static final long serialVersionUID = 1L;

        final String name;
        final String stateType;
        @SuppressWarnings("rawtypes")
        final TypeSerializer valueSerializer;
        @SuppressWarnings("rawtypes")
        final TypeSerializer mapKeySerializer;

        @SuppressWarnings("rawtypes")
        StateDescriptorEntry(String name, String stateType,
                             TypeSerializer valueSerializer, TypeSerializer mapKeySerializer) {
            this.name = name;
            this.stateType = stateType;
            this.valueSerializer = valueSerializer;
            this.mapKeySerializer = mapKeySerializer;
        }
    }
}
