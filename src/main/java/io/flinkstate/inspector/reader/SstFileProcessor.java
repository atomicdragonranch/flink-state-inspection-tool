package io.flinkstate.inspector.reader;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads RocksDB SST files and deserializes keyed state entries.
 * Handles opening SST files, iterating entries, reading column family names,
 * and mapping SST files to state descriptors.
 */
final class SstFileProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileProcessor.class);

    private static final int KEY_SCAN_MULTIPLIER = 10;

    private SstFileProcessor() {
    }

    static SstScanResult scanSstFiles(
            List<String> sstFiles,
            Map<String, GenericStateReader.StateDescriptorEntry> stateByName,
            List<StateMetaInfoSnapshot> stateInfos,
            TypeSerializer<?> keySerializer,
            int keyGroupPrefixBytes,
            boolean ambiguousKeyPossible,
            String keyFilter,
            int limit) {

        Map<String, Map<String, Object>> keyEntries = new LinkedHashMap<>();
        int maxKeys = limit * KEY_SCAN_MULTIPLIER;
        boolean truncated = false;
        int skippedFrocksdbFiles = 0;
        List<String> warnings = new ArrayList<>();

        for (String sstFile : sstFiles) {
            try (Options opts = new Options();
                 SstFileReader reader = new SstFileReader(opts)) {
                reader.open(sstFile);
                byte[] cfNameBytes = reader.getTableProperties().getColumnFamilyName();
                String cfName = new String(cfNameBytes, StandardCharsets.UTF_8);

                GenericStateReader.StateDescriptorEntry sde = stateByName.get(cfName);
                if (sde == null) continue;

                TypeSerializer<?> nsSerializer = null;
                if ("MAP".equals(sde.stateType)) {
                    for (StateMetaInfoSnapshot info : stateInfos) {
                        if (info.getName().equals(cfName)) {
                            var nsSnap = info.getTypeSerializerSnapshot(
                                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER);
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

                        processEntry(sde, entry, rawKey, rawValue,
                            keyGroupPrefixBytes, keySerializer, nsSerializer);

                        iter.next();
                    }
                }
            } catch (RocksDBException e) {
                if (GenericStateReader.isFrocksdbFormatError(e)) {
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

        return new SstScanResult(keyEntries, skippedFrocksdbFiles, warnings);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void processEntry(
            GenericStateReader.StateDescriptorEntry sde,
            Map<String, Object> entry,
            byte[] rawKey, byte[] rawValue,
            int keyGroupPrefixBytes,
            TypeSerializer keySerializer,
            TypeSerializer nsSerializer) {

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
                    entry.put(sde.name, GenericStateReader.rawBytesMap(rawValue,
                        ex.getClass().getSimpleName()
                        + ": " + ex.getMessage()));
                }
            } else {
                Object value = GenericStateReader.deserializeValue(sde, rawValue);
                entry.put(sde.name, value);
            }
        } else {
            Object value = GenericStateReader.deserializeValue(sde, rawValue);
            entry.put(sde.name, value);
        }
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

    static void cleanupTempFiles(List<File> tempSstFiles) {
        for (File tempFile : tempSstFiles) {
            try {
                Files.deleteIfExists(tempFile.toPath());
            } catch (IOException e) {
                LOG.debug("Failed to delete temp SST file: {}", tempFile, e);
            }
        }
    }

    static final class SstScanResult {
        final Map<String, Map<String, Object>> keyEntries;
        final int skippedFrocksdbFiles;
        final List<String> warnings;

        SstScanResult(Map<String, Map<String, Object>> keyEntries,
                      int skippedFrocksdbFiles, List<String> warnings) {
            this.keyEntries = keyEntries;
            this.skippedFrocksdbFiles = skippedFrocksdbFiles;
            this.warnings = warnings;
        }
    }
}
