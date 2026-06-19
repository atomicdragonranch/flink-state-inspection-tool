package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DiffEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DiffEndpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DiffEndpoint() {
    }

    public static void register(Javalin app) {
        app.post("/api/diff/keyed", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path1 = requireField(body, "path1");
            String path2 = requireField(body, "path2");
            String operatorUid = requireField(body, "operatorUid");

            // TODO: wire to DiffExecutor + auto-discovery (#1, #2, #6)
            LOG.info("Diff keyed: path1={}, path2={}, operator={}", path1, path2, operatorUid);

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("operatorName", operatorUid);
            data.put("label1", extractLabel(path1));
            data.put("label2", extractLabel(path2));
            data.put("added", Collections.emptyList());
            data.put("removed", Collections.emptyList());
            data.put("modified", Collections.emptyList());
            data.put("unchangedCount", 0);
            data.put("totalKeys", 0);
            ctx.json(ApiResponse.success(data));
        });

        app.post("/api/diff/broadcast", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path1 = requireField(body, "path1");
            String path2 = requireField(body, "path2");
            String stateName = requireField(body, "stateName");

            LOG.info("Diff broadcast: path1={}, path2={}, state={}", path1, path2, stateName);

            Map<String, Object> data = new LinkedHashMap<>();
            data.put("operatorName", stateName);
            data.put("label1", extractLabel(path1));
            data.put("label2", extractLabel(path2));
            data.put("added", Collections.emptyList());
            data.put("removed", Collections.emptyList());
            data.put("modified", Collections.emptyList());
            data.put("unchangedCount", 0);
            data.put("totalKeys", 0);
            ctx.json(ApiResponse.success(data));
        });
    }

    private static String extractLabel(String path) {
        if (path.contains("/")) {
            return path.substring(path.lastIndexOf('/') + 1);
        }
        return path;
    }

    private static String requireField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.asText().isEmpty()) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return node.asText();
    }
}
