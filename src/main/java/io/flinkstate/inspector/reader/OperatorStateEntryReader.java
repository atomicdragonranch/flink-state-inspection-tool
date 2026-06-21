package io.flinkstate.inspector.reader;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads broadcast and list state entries from operator state handles.
 */
final class OperatorStateEntryReader {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorStateEntryReader.class);

    private OperatorStateEntryReader() {
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
                entry.put("key", OperatorStateReader.presentValue(key));
                entry.put("value", OperatorStateReader.presentValue(value));
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
                entry.put("value", OperatorStateReader.presentValue(value));
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
}
