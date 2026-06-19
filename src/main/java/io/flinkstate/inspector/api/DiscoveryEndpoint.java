package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.storage.DockerStorageConnector;
import io.flinkstate.inspector.storage.StorageConnector;
import io.flinkstate.inspector.storage.StorageConnectorFactory;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DiscoveryEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryEndpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_LIMIT = 50;
    private static final String[] COMMON_CHECKPOINT_PATHS = {
        "/tmp/flink-checkpoints",
        "/opt/flink/checkpoints",
        "/checkpoints",
        "/tmp/flink-savepoints",
        "/opt/flink/savepoints"
    };

    private DiscoveryEndpoint() {
    }

    public static void register(Javalin app) {
        app.get("/api/sources/detect", ctx -> {
            List<Map<String, Object>> sources = detectSources();
            ctx.json(ApiResponse.success(sources));
        });

        app.post("/api/checkpoints/discover", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path = requireField(body, "path");
            int limit = intField(body, "totalLimit", DEFAULT_LIMIT);

            try (StorageConnector connector = StorageConnectorFactory.create(path)) {
                List<CheckpointEntry> checkpoints = connector.discoverCheckpoints(path, limit);
                ctx.json(ApiResponse.success(toSnapshotResponse(checkpoints, connector)));
            }
        });

        app.post("/api/savepoints/discover", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path = requireField(body, "path");
            int limit = intField(body, "totalLimit", DEFAULT_LIMIT);

            try (StorageConnector connector = StorageConnectorFactory.create(path)) {
                List<CheckpointEntry> savepoints = connector.discoverCheckpoints(path, limit);
                ctx.json(ApiResponse.success(toSnapshotResponse(savepoints, connector)));
            }
        });
    }

    private static List<Map<String, Object>> toSnapshotResponse(
            List<CheckpointEntry> entries, StorageConnector connector) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (CheckpointEntry entry : entries) {
            Map<String, Object> cp = new LinkedHashMap<>();
            cp.put("jobId", entry.getJobId());
            cp.put("shortJobId", entry.shortJobId());
            cp.put("path", entry.getPath());
            cp.put("modificationTime", entry.getModificationTime());
            cp.put("formattedTime", entry.formattedTime());
            cp.put("type", entry.getType().name().toLowerCase());
            cp.put("displayLabel", entry.displayName());
            cp.put("valid", connector.validateCheckpoint(entry.getPath()));
            result.add(cp);
        }
        return result;
    }

    private static String requireField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.asText().isEmpty()) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return node.asText();
    }

    private static int intField(JsonNode body, String fieldName, int defaultValue) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        return node.asInt(defaultValue);
    }

    private static List<Map<String, Object>> detectSources() {
        List<Map<String, Object>> sources = new ArrayList<>();
        try {
            List<String> containers = listDockerContainers();
            for (String container : containers) {
                for (String checkpointPath : COMMON_CHECKPOINT_PATHS) {
                    if (hasDirectory(container, checkpointPath)) {
                        Map<String, Object> source = new LinkedHashMap<>();
                        source.put("type", "docker");
                        source.put("container", container);
                        source.put("path", "docker://" + container + checkpointPath);
                        source.put("label", container + ":" + checkpointPath);
                        sources.add(source);
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("Docker auto-detection unavailable: {}", e.getMessage());
        }
        return sources;
    }

    private static List<String> listDockerContainers() throws Exception {
        Process process = new ProcessBuilder(
            "docker", "ps", "--format", "{{.Names}}", "--filter", "status=running")
            .redirectErrorStream(true)
            .start();
        List<String> names = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) names.add(trimmed);
            }
        }
        process.waitFor();
        return names;
    }

    private static boolean hasDirectory(String container, String path) {
        try {
            Process process = new ProcessBuilder(
                "docker", "exec", container, "test", "-d", path)
                .redirectErrorStream(true)
                .start();
            return process.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
