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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.flinkstate.inspector.api.RequestParser.intField;
import static io.flinkstate.inspector.api.RequestParser.requireField;

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
            Map<String, String> config = extractConfig(body);

            try (StorageConnector connector = StorageConnectorFactory.create(path, config)) {
                List<CheckpointEntry> checkpoints = connector.discoverCheckpoints(path, limit);
                ctx.json(ApiResponse.success(toSnapshotResponse(checkpoints, connector)));
            }
        });

        app.post("/api/savepoints/discover", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path = requireField(body, "path");
            int limit = intField(body, "totalLimit", DEFAULT_LIMIT);
            Map<String, String> config = extractConfig(body);

            try (StorageConnector connector = StorageConnectorFactory.create(path, config)) {
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

    static Map<String, String> extractConfig(JsonNode body) {
        JsonNode configNode = body.get("config");
        if (configNode == null || !configNode.isObject()) {
            return Collections.emptyMap();
        }
        Map<String, String> config = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = configNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            config.put(field.getKey(), field.getValue().asText());
        }
        return config;
    }

    private static List<Map<String, Object>> detectSources() {
        List<Map<String, Object>> sources = new ArrayList<>();
        detectDockerSources(sources);
        detectS3Sources(sources);
        return sources;
    }

    private static void detectDockerSources(List<Map<String, Object>> sources) {
        try {
            List<String> containers = listDockerContainers();
            for (String container : containers) {
                for (String checkpointPath : COMMON_CHECKPOINT_PATHS) {
                    if (hasDirectory(container, checkpointPath)) {
                        String dockerPath = "docker://" + container + checkpointPath;
                        int snapshotCount = probeSnapshotCount(dockerPath);
                        Map<String, Object> source = new LinkedHashMap<>();
                        source.put("type", "docker");
                        source.put("container", container);
                        source.put("path", dockerPath);
                        source.put("label", container + ":" + checkpointPath);
                        source.put("snapshotCount", snapshotCount);
                        sources.add(source);
                    }
                }
            }
        } catch (Exception e) {
            LOG.debug("Docker auto-detection unavailable: {}", e.getMessage());
        }
    }

    private static void detectS3Sources(List<Map<String, Object>> sources) {
        String s3Paths = System.getenv("FLINK_S3_CHECKPOINT_PATHS");
        if (s3Paths == null || s3Paths.isEmpty()) {
            return;
        }
        for (String rawPath : s3Paths.split(",")) {
            String path = rawPath.trim();
            if (path.isEmpty()) continue;
            try (StorageConnector connector = StorageConnectorFactory.create(path)) {
                List<CheckpointEntry> found = connector.discoverCheckpoints(path, 1);
                String label = path.replaceFirst("^s3a?://", "");
                Map<String, Object> source = new LinkedHashMap<>();
                source.put("type", "s3");
                source.put("path", path);
                source.put("label", label);
                source.put("snapshotCount", found.size());
                sources.add(source);
                LOG.info("Detected S3 source: {}", path);
            } catch (Exception e) {
                LOG.debug("S3 source not accessible: {} ({})", path, e.getMessage());
            }
        }
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
        boolean completed = process.waitFor(5, TimeUnit.SECONDS);
        if (!completed) {
            process.destroyForcibly();
            LOG.warn("Docker command timed out");
            return Collections.emptyList();
        }
        return names;
    }

    private static int probeSnapshotCount(String path) {
        try (StorageConnector connector = StorageConnectorFactory.create(path)) {
            return connector.discoverCheckpoints(path, 10).size();
        } catch (Exception e) {
            LOG.debug("Could not probe snapshots at {}: {}", path, e.getMessage());
            return 0;
        }
    }

    private static boolean hasDirectory(String container, String path) {
        try {
            Process process = new ProcessBuilder(
                "docker", "exec", container, "test", "-d", path)
                .redirectErrorStream(true)
                .start();
            boolean completed = process.waitFor(5, TimeUnit.SECONDS);
            if (!completed) {
                process.destroyForcibly();
                LOG.warn("Docker command timed out");
                return false;
            }
            return process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
