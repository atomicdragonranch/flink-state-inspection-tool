package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flinkstate.inspector.reader.GenericStateReader;
import io.flinkstate.inspector.reader.OperatorStateReader;
import io.flinkstate.inspector.reader.StateReadResult;
import io.flinkstate.inspector.storage.CheckpointCache;
import io.flinkstate.inspector.storage.StorageConnector;
import io.flinkstate.inspector.storage.StorageConnectorFactory;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static io.flinkstate.inspector.api.RequestParser.intField;
import static io.flinkstate.inspector.api.RequestParser.intFieldAllowZero;
import static io.flinkstate.inspector.api.RequestParser.optionalField;
import static io.flinkstate.inspector.api.RequestParser.requireField;

public final class DiffEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DiffEndpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_LIMIT = 10_000;

    private DiffEndpoint() {
    }

    public static void register(Javalin app) {
        app.post("/api/diff/keyed", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path1 = requireField(body, "path1");
            String path2 = requireField(body, "path2");
            String operatorUid = requireField(body, "operatorUid");
            String keyFilter = optionalField(body, "keyFilter");
            int limit = intField(body, "limit", DEFAULT_LIMIT);
            int offset = intFieldAllowZero(body, "offset", 0);

            LOG.info("Diff keyed: path1={}, path2={}, operator={}, offset={}, limit={}",
                path1, path2, operatorUid, offset, limit);

            Map<String, String> config = DiscoveryEndpoint.extractConfig(body);
            String local1 = resolveLocalPath(path1, config);
            String local2 = resolveLocalPath(path2, config);

            StateReadResult result1 = GenericStateReader.readKeyedState(
                local1, operatorUid, keyFilter, false, DEFAULT_LIMIT);
            StateReadResult result2 = GenericStateReader.readKeyedState(
                local2, operatorUid, keyFilter, false, DEFAULT_LIMIT);

            Map<String, Object> data = computeDiff(
                result1, result2, operatorUid, path1, path2, offset, limit);
            ctx.json(ApiResponse.success(data));
        });

        app.post("/api/diff/broadcast", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path1 = requireField(body, "path1");
            String path2 = requireField(body, "path2");
            String operatorUid = requireField(body, "operatorUid");
            String stateName = requireField(body, "stateName");
            String keyFilter = optionalField(body, "keyFilter");
            int limit = intField(body, "limit", DEFAULT_LIMIT);
            int offset = intFieldAllowZero(body, "offset", 0);

            LOG.info("Diff broadcast: path1={}, path2={}, operator={}, state={}, offset={}, limit={}",
                path1, path2, operatorUid, stateName, offset, limit);

            Map<String, String> config = DiscoveryEndpoint.extractConfig(body);
            String local1 = resolveLocalPath(path1, config);
            String local2 = resolveLocalPath(path2, config);

            StateReadResult result1 = OperatorStateReader.readOperatorState(
                local1, operatorUid, stateName, keyFilter, DEFAULT_LIMIT);
            StateReadResult result2 = OperatorStateReader.readOperatorState(
                local2, operatorUid, stateName, keyFilter, DEFAULT_LIMIT);

            Map<String, Object> data = computeDiff(
                result1, result2, operatorUid, path1, path2, offset, limit);
            ctx.json(ApiResponse.success(data));
        });
    }

    static Map<String, Object> computeDiff(
            StateReadResult result1, StateReadResult result2,
            String operatorUid, String path1, String path2) {
        return computeDiff(result1, result2, operatorUid, path1, path2, 0, DEFAULT_LIMIT);
    }

    static Map<String, Object> computeDiff(
            StateReadResult result1, StateReadResult result2,
            String operatorUid, String path1, String path2,
            int offset, int limit) {

        Map<String, Map<String, Object>> map1 = indexByKey(result1.getEntries());
        Map<String, Map<String, Object>> map2 = indexByKey(result2.getEntries());

        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(map1.keySet());
        allKeys.addAll(map2.keySet());

        List<Map<String, Object>> added = new ArrayList<>();
        List<Map<String, Object>> removed = new ArrayList<>();
        List<Map<String, Object>> modified = new ArrayList<>();
        int unchangedCount = 0;

        for (String key : allKeys) {
            Map<String, Object> entry1 = map1.get(key);
            Map<String, Object> entry2 = map2.get(key);

            if (entry1 == null) {
                added.add(diffEntry(key, null, entry2));
            } else if (entry2 == null) {
                removed.add(diffEntry(key, entry1, null));
            } else if (entriesEqual(entry1, entry2)) {
                unchangedCount++;
            } else {
                Map<String, Object> diff = diffEntry(key, entry1, entry2);
                diff.put("fieldChanges", computeFieldChanges(entry1, entry2));
                modified.add(diff);
            }
        }

        int totalAdded = added.size();
        int totalRemoved = removed.size();
        int totalModified = modified.size();

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("operatorName", operatorUid);
        data.put("label1", extractLabel(path1));
        data.put("label2", extractLabel(path2));
        data.put("added", paginate(added, offset, limit));
        data.put("removed", paginate(removed, offset, limit));
        data.put("modified", paginate(modified, offset, limit));
        data.put("unchangedCount", unchangedCount);
        data.put("totalKeys", allKeys.size());
        data.put("totalAdded", totalAdded);
        data.put("totalRemoved", totalRemoved);
        data.put("totalModified", totalModified);
        data.put("offset", offset);
        return data;
    }

    private static <T> List<T> paginate(List<T> list, int offset, int limit) {
        int size = list.size();
        int from = Math.min(offset, size);
        int to = Math.min(offset + limit, size);
        return list.subList(from, to);
    }

    private static Map<String, Map<String, Object>> indexByKey(
            List<Map<String, Object>> entries) {
        Map<String, Map<String, Object>> index = new LinkedHashMap<>();
        for (Map<String, Object> entry : entries) {
            Object keyObj = entry.get("key");
            if (keyObj == null) {
                keyObj = entry.get("partition");
            }
            String key = String.valueOf(keyObj);
            index.put(key, entry);
        }
        return index;
    }

    private static Map<String, Object> diffEntry(
            String key, Map<String, Object> entry1, Map<String, Object> entry2) {
        Map<String, Object> diff = new LinkedHashMap<>();
        diff.put("key", key);
        diff.put("json1", entry1);
        diff.put("json2", entry2);
        return diff;
    }

    private static boolean entriesEqual(
            Map<String, Object> entry1, Map<String, Object> entry2) {
        try {
            String json1 = MAPPER.writeValueAsString(entry1);
            String json2 = MAPPER.writeValueAsString(entry2);
            return json1.equals(json2);
        } catch (Exception e) {
            return Objects.equals(entry1, entry2);
        }
    }

    private static List<Map<String, Object>> computeFieldChanges(
            Map<String, Object> entry1, Map<String, Object> entry2) {
        List<Map<String, Object>> changes = new ArrayList<>();

        Set<String> allFields = new LinkedHashSet<>();
        allFields.addAll(entry1.keySet());
        allFields.addAll(entry2.keySet());
        allFields.remove("key");
        allFields.remove("partition");

        for (String field : allFields) {
            Object val1 = entry1.get(field);
            Object val2 = entry2.get(field);

            String str1 = valueToString(val1);
            String str2 = valueToString(val2);

            if (!Objects.equals(str1, str2)) {
                Map<String, Object> change = new LinkedHashMap<>();
                change.put("fieldName", field);
                change.put("oldValue", str1);
                change.put("newValue", str2);
                changes.add(change);
            }
        }
        return changes;
    }

    private static String valueToString(Object value) {
        if (value == null) return null;
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private static String resolveLocalPath(String path, Map<String, String> config)
            throws Exception {
        String cached = CheckpointCache.getInstance().lookupLocalPath(path);
        if (cached != null) return cached;
        try (StorageConnector connector = StorageConnectorFactory.create(path, config)) {
            String localPath = connector.resolveFullCheckpoint(path);
            CheckpointCache.getInstance().register(path, localPath);
            return localPath;
        }
    }

    private static String extractLabel(String path) {
        if (path.contains("/")) {
            return path.substring(path.lastIndexOf('/') + 1);
        }
        return path;
    }

}
