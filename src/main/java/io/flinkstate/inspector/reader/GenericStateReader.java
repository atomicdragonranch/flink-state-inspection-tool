package io.flinkstate.inspector.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.ArrayListSerializer;
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
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class GenericStateReader {

    private static final Logger LOG = LoggerFactory.getLogger(GenericStateReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LenientClassLoader LENIENT_CL =
        new LenientClassLoader(GenericStateReader.class.getClassLoader());

    // Scan extra keys to account for key-group distribution across SST files
    private static final int KEY_SCAN_MULTIPLIER = 10;
    private static final int HEX_DISPLAY_MAX_BYTES = 256;
    private static final int BASE64_INCLUDE_MAX_BYTES = 1024;

    private GenericStateReader() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static StateReadResult readKeyedState(
            String localCheckpointPath, String operatorUidOrHash,
            String keyFilter, boolean keysOnly, int limit) throws Exception {

        ClassLoader originalCl = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(LENIENT_CL);

            OperatorInfo opInfo = discoverOperatorInfo(localCheckpointPath, operatorUidOrHash);

            Map<String, StateDescriptorEntry> stateByName = new HashMap<>();
            List<StateDescriptorEntry> sdEntries = new ArrayList<>();
            for (StateMetaInfoSnapshot info : opInfo.stateInfos) {
                if (info.getBackendStateType() != StateMetaInfoSnapshot.BackendStateType.KEY_VALUE) {
                    continue;
                }
                String stateType = info.getOption(
                    StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE);

                TypeSerializerSnapshot<?> valueSnapshot = info
                    .getTypeSerializerSnapshot(
                        StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER);
                if (valueSnapshot == null) {
                    LOG.warn("Skipping state '{}': no VALUE_SERIALIZER snapshot (type={})",
                        info.getName(), stateType);
                    continue;
                }
                TypeSerializer valueSerializer = valueSnapshot.restoreSerializer();

                TypeSerializer mapKeySerializer = null;
                TypeSerializer mapValueSerializer = null;
                TypeSerializer listElementSerializer = null;
                if ("MAP".equals(stateType)) {
                    if (valueSerializer instanceof MapSerializer) {
                        MapSerializer<?, ?> mapSer = (MapSerializer<?, ?>) valueSerializer;
                        mapKeySerializer = mapSer.getKeySerializer();
                        mapValueSerializer = mapSer.getValueSerializer();
                    }
                } else if ("LIST".equals(stateType)) {
                    if (valueSerializer instanceof ListSerializer) {
                        listElementSerializer =
                            ((ListSerializer<?>) valueSerializer).getElementSerializer();
                    } else if (valueSerializer instanceof ArrayListSerializer) {
                        listElementSerializer =
                            ((ArrayListSerializer<?>) valueSerializer).getElementSerializer();
                    }
                }

                StateDescriptorEntry sde = new StateDescriptorEntry(
                    info.getName(), stateType, valueSerializer,
                    mapKeySerializer, mapValueSerializer, listElementSerializer);
                sdEntries.add(sde);
                stateByName.put(info.getName(), sde);
            }

            TypeSerializer keySerializer = opInfo.keySerializerSnapshot.restoreSerializer();
            int keyGroupPrefixBytes = CompositeKeySerializationUtils
                .computeRequiredBytesInKeyGroupPrefix(opInfo.maxParallelism);
            boolean ambiguousKeyPossible = CompositeKeySerializationUtils
                .isAmbiguousKeyPossible(keySerializer, keySerializer);

            List<File> tempSstFiles = new ArrayList<>();
            List<String> sstFiles = resolveSstFiles(opInfo.stateHandle, localCheckpointPath, tempSstFiles);

            Map<String, Map<String, Object>> keyEntries = new LinkedHashMap<>();
            int maxKeys = limit * KEY_SCAN_MULTIPLIER;
            boolean truncated = false;
            int skippedFrocksdbFiles = 0;
            List<String> warnings = new ArrayList<>();

            try {
                for (String sstFile : sstFiles) {
                    try (Options opts = new Options();
                         SstFileReader reader = new SstFileReader(opts)) {
                        reader.open(sstFile);
                        byte[] cfNameBytes = reader.getTableProperties().getColumnFamilyName();
                        String cfName = new String(cfNameBytes, StandardCharsets.UTF_8);

                        StateDescriptorEntry sde = stateByName.get(cfName);
                        if (sde == null) continue;

                        TypeSerializer nsSerializer = null;
                        if ("MAP".equals(sde.stateType)) {
                            for (StateMetaInfoSnapshot info : opInfo.stateInfos) {
                                if (info.getName().equals(cfName)) {
                                    TypeSerializerSnapshot<?> nsSnap = info
                                        .getTypeSerializerSnapshot(
                                            StateMetaInfoSnapshot.CommonSerializerKeys
                                                .NAMESPACE_SERIALIZER);
                                    if (nsSnap != null) {
                                        nsSerializer = nsSnap.restoreSerializer();
                                    }
                                    break;
                                }
                            }
                        }

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

                                if (!keyEntries.containsKey(keyStr)
                                        && keyEntries.size() >= maxKeys) {
                                    truncated = true;
                                    iter.next();
                                    continue;
                                }

                                Map<String, Object> entry = keyEntries.computeIfAbsent(
                                    keyStr, k -> {
                                        Map<String, Object> e = new LinkedHashMap<>();
                                        e.put("key", key);
                                        return e;
                                    });

                                if ("MAP".equals(sde.stateType)
                                        && sde.mapKeySerializer != null
                                        && sde.mapValueSerializer != null) {
                                    Object mapKey = extractMapKey(rawKey, keyGroupPrefixBytes,
                                        keySerializer, nsSerializer, sde.mapKeySerializer);
                                    if (mapKey != null) {
                                        try {
                                            DataInputDeserializer valInput =
                                                new DataInputDeserializer(rawValue);
                                            if (rawValue.length > 0 && rawValue[0] == 0x00) {
                                                valInput.skipBytesToRead(1);
                                            }
                                            Object mapVal = sde.mapValueSerializer
                                                .deserialize(valInput);
                                            @SuppressWarnings("unchecked")
                                            Map<String, Object> map = (Map<String, Object>)
                                                entry.computeIfAbsent(sde.name,
                                                    n -> new LinkedHashMap<>());
                                            map.put(String.valueOf(mapKey), mapVal);
                                        } catch (Exception ex) {
                                            entry.put(sde.name, rawBytesMap(rawValue,
                                                ex.getClass().getSimpleName()
                                                + ": " + ex.getMessage()));
                                        }
                                    } else {
                                        Object value = deserializeValue(sde, rawValue);
                                        entry.put(sde.name, value);
                                    }
                                } else {
                                    Object value = deserializeValue(sde, rawValue);
                                    entry.put(sde.name, value);
                                }

                                iter.next();
                            }
                        }
                    } catch (RocksDBException e) {
                        if (isFrocksdbFormatError(e)) {
                            skippedFrocksdbFiles++;
                            LOG.warn("SST file appears to be in FRocksDB format (used by most "
                                + "production Flink deployments). This inspector was built with "
                                + "standard RocksDB. To read FRocksDB-format files, rebuild with "
                                + "frocksdbjni instead of rocksdbjni in pom.xml. "
                                + "Skipping file: {}", sstFile);
                        } else {
                            LOG.warn("Failed to read SST file {}: {}", sstFile, e.getMessage());
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to read SST file {}: {}", sstFile, e.getMessage());
                    }
                }
            } finally {
                for (File tempFile : tempSstFiles) {
                    try {
                        Files.deleteIfExists(tempFile.toPath());
                    } catch (IOException e) {
                        LOG.debug("Failed to delete temp SST file: {}", tempFile, e);
                    }
                }
            }

            if (skippedFrocksdbFiles > 0) {
                String msg = String.format(
                    "%d SST file(s) skipped: FRocksDB format not readable by standard "
                    + "RocksDB. Rebuild with 'mvn package -Pfrocksdb' for FRocksDB support.",
                    skippedFrocksdbFiles);
                warnings.add(msg);
                LOG.warn("{} SST file(s) skipped: FRocksDB format not readable by standard RocksDB. "
                    + "Rebuild with 'mvn package -Pfrocksdb' for FRocksDB support.", skippedFrocksdbFiles);
            }

            if (truncated) {
                LOG.warn("Keyed state scan capped at {} unique keys (limit={}); "
                    + "results may be incomplete", maxKeys, limit);
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

            return new StateReadResult(operatorUidOrHash, results, columns,
                skippedFrocksdbFiles, warnings);
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
                case "REDUCING":
                case "AGGREGATING":
                    return sde.valueSerializer.deserialize(valueInput);
                case "LIST": {
                    TypeSerializer elemSer = sde.listElementSerializer != null
                        ? sde.listElementSerializer : sde.valueSerializer;
                    List<Object> items = new ArrayList<>();
                    while (valueInput.available() > 0) {
                        items.add(elemSer.deserialize(valueInput));
                        if (valueInput.available() > 0) {
                            valueInput.skipBytesToRead(1);
                        }
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
        result.put("_hex", bytesToHex(rawValue, HEX_DISPLAY_MAX_BYTES));
        if (rawValue.length <= BASE64_INCLUDE_MAX_BYTES) {
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

    static boolean isFrocksdbFormatError(RocksDBException e) {
        String msg = e.getMessage();
        if (msg == null) return false;
        String lower = msg.toLowerCase();
        return lower.contains("magic number")
            || lower.contains("bad table magic number")
            || lower.contains("not an sstable")
            || (lower.contains("corruption") && lower.contains("sstable"));
    }

    private static List<String> resolveSstFiles(
            IncrementalRemoteKeyedStateHandle handle,
            String localCheckpointPath,
            List<File> tempFilesOut) throws Exception {
        List<String> paths = new ArrayList<>();
        for (HandleAndLocalPath hp : handle.getSharedState()) {
            String lp = hp.getLocalPath();
            if (lp != null && !lp.endsWith(".sst") && !isSstHandle(hp)) continue;
            String resolved = resolveHandlePath(hp.getHandle(), localCheckpointPath, tempFilesOut);
            if (resolved != null) paths.add(resolved);
        }
        for (HandleAndLocalPath hp : handle.getPrivateState()) {
            String lp = hp.getLocalPath();
            if (lp != null && !lp.endsWith(".sst") && !isSstHandle(hp)) continue;
            String resolved = resolveHandlePath(hp.getHandle(), localCheckpointPath, tempFilesOut);
            if (resolved != null) paths.add(resolved);
        }
        return paths;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object extractMapKey(byte[] rawKey, int keyGroupPrefixBytes,
            TypeSerializer keySerializer, TypeSerializer nsSerializer,
            TypeSerializer mapKeySerializer) {
        int keyStart = keyGroupPrefixBytes;
        int keyLen = rawKey.length - keyStart;
        if (keyLen < 2) return null;
        try {
            DataInputDeserializer in = new DataInputDeserializer(rawKey, keyStart, keyLen);
            keySerializer.deserialize(in);
            if (nsSerializer != null) {
                nsSerializer.deserialize(in);
            }
            if (in.available() > 0) {
                return mapKeySerializer.deserialize(in);
            }
        } catch (Exception e) {
            LOG.debug("Failed to extract map key from {} bytes: {}",
                rawKey.length, e.getMessage());
        }
        return null;
    }

    private static boolean isSstHandle(HandleAndLocalPath hp) {
        StreamStateHandle h = hp.getHandle();
        if (h instanceof FileStateHandle) {
            String name = ((FileStateHandle) h).getFilePath().getName();
            return name.endsWith(".sst") || !name.contains(".");
        }
        return true;
    }

    private static String resolveHandlePath(
            StreamStateHandle handle, String localCheckpointPath,
            List<File> tempFilesOut) throws Exception {
        if (handle instanceof RelativeFileStateHandle) {
            String fileName = ((RelativeFileStateHandle) handle).getRelativePath();
            File f = new File(localCheckpointPath, fileName);
            if (f.exists()) return f.getAbsolutePath();
        } else if (handle instanceof FileStateHandle) {
            String fileName = ((FileStateHandle) handle).getFilePath().getName();
            File f = new File(localCheckpointPath, fileName);
            if (f.exists()) return f.getAbsolutePath();
            File shared = new File(localCheckpointPath, "shared/" + fileName);
            if (shared.exists()) return shared.getAbsolutePath();
        }
        try (InputStream in = handle.openInputStream()) {
            File tempFile = File.createTempFile("sst-", ".sst",
                new File(localCheckpointPath));
            tempFilesOut.add(tempFile);
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

        File metadataFile = new File(localCheckpointPath, "_metadata");
        CheckpointMetadata metadata;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(metadataFile)))) {
            metadata = Checkpoints.loadCheckpointMetadata(
                dis, LENIENT_CL, localCheckpointPath);
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
                                new KeyedBackendSerializationProxy<>(LENIENT_CL);
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

    static InputStream openMetaHandle(
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
        final TypeSerializer mapValueSerializer;
        @SuppressWarnings("rawtypes")
        final TypeSerializer listElementSerializer;

        @SuppressWarnings("rawtypes")
        StateDescriptorEntry(String name, String stateType,
                             TypeSerializer valueSerializer,
                             TypeSerializer mapKeySerializer,
                             TypeSerializer mapValueSerializer,
                             TypeSerializer listElementSerializer) {
            this.name = name;
            this.stateType = stateType;
            this.valueSerializer = valueSerializer;
            this.mapKeySerializer = mapKeySerializer;
            this.mapValueSerializer = mapValueSerializer;
            this.listElementSerializer = listElementSerializer;
        }
    }
}
