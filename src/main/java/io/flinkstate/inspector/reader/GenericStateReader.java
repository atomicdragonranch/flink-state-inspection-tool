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
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class GenericStateReader {

    private static final Logger LOG = LoggerFactory.getLogger(GenericStateReader.class);
    private static final LenientClassLoader LENIENT_CL =
        new LenientClassLoader(GenericStateReader.class.getClassLoader());

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
            List<String> sstFiles = HandleResolver.resolveSstFiles(
                opInfo.stateHandle, localCheckpointPath, tempSstFiles);

            try {
                SstFileProcessor.SstScanResult scanResult = SstFileProcessor.scanSstFiles(
                    sstFiles, stateByName, opInfo.stateInfos,
                    keySerializer, keyGroupPrefixBytes, ambiguousKeyPossible,
                    keyFilter, limit);

                List<Map<String, Object>> results = new ArrayList<>();
                for (Map<String, Object> entry : scanResult.keyEntries.values()) {
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
                    scanResult.skippedFrocksdbFiles, scanResult.warnings);
            } finally {
                SstFileProcessor.cleanupTempFiles(tempSstFiles);
            }
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
