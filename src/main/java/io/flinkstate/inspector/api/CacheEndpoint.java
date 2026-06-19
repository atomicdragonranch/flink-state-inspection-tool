package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flinkstate.inspector.storage.CheckpointCache;
import io.javalin.Javalin;

public final class CacheEndpoint {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CacheEndpoint() {
    }

    public static void register(Javalin app) {
        app.get("/api/cache/list", ctx -> {
            ctx.json(ApiResponse.success(CheckpointCache.getInstance().listEntries()));
        });

        app.post("/api/cache/delete", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            JsonNode pathNode = body.get("localPath");
            if (pathNode == null || pathNode.asText().isEmpty()) {
                throw new IllegalArgumentException("Missing required field: localPath");
            }
            boolean deleted = CheckpointCache.getInstance().delete(pathNode.asText());
            ctx.json(ApiResponse.success(deleted));
        });
    }
}
