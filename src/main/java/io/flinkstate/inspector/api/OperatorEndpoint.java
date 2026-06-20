package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flinkstate.inspector.discovery.DiscoveredOperator;
import io.flinkstate.inspector.discovery.MetadataReader;
import io.flinkstate.inspector.storage.CheckpointCache;
import io.flinkstate.inspector.storage.StorageConnector;
import io.flinkstate.inspector.storage.StorageConnectorFactory;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OperatorEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorEndpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OperatorEndpoint() {
    }

    public static void register(Javalin app) {
        app.post("/api/operators/discover", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path = requireField(body, "path");

            LOG.info("Discover operators: path={}", path);

            Map<String, String> config = DiscoveryEndpoint.extractConfig(body);
            StorageConnector connector = StorageConnectorFactory.create(path, config);
            String localPath = connector.resolveFullCheckpoint(path);
            CheckpointCache.getInstance().register(path, localPath);
            List<DiscoveredOperator> operators = MetadataReader.readOperatorsFromPath(
                localPath);

            int sstCount = countSstFiles(new java.io.File(localPath));
            for (DiscoveredOperator op : operators) {
                if (!op.getKeyedStates().isEmpty()) {
                    op.setKeyedStateEntryCount(sstCount);
                }
            }

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("operators", operators);
            data.put("localPath", localPath);
            ctx.json(ApiResponse.success(data));
        });
    }

    private static int countSstFiles(java.io.File checkpointDir) {
        int count = 0;
        java.io.File[] files = checkpointDir.listFiles();
        if (files == null) return 0;
        for (java.io.File f : files) {
            if (f.isFile() && f.getName().endsWith(".sst")) {
                count++;
            } else if (f.isDirectory()) {
                count += countSstFiles(f);
            }
        }
        return count;
    }

    private static String requireField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.asText().isEmpty()) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return node.asText();
    }
}
