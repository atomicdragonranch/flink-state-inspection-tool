package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flinkstate.inspector.storage.CheckpointCache;
import io.javalin.Javalin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CacheEndpoint {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_LIMIT = 100;

    private CacheEndpoint() {
    }

    public static void register(Javalin app) {
        app.get("/api/cache/list", ctx -> {
            int limit = parseQueryInt(ctx.queryParam("limit"), DEFAULT_LIMIT, 1);
            int offset = parseQueryInt(ctx.queryParam("offset"), 0, 0);

            List<Map<String, Object>> allEntries = CheckpointCache.getInstance().listEntries();
            int totalCount = allEntries.size();
            int from = Math.min(offset, totalCount);
            int to = Math.min(offset + limit, totalCount);
            List<Map<String, Object>> paged = allEntries.subList(from, to);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("totalCount", totalCount);
            result.put("offset", offset);
            result.put("entries", paged);
            ctx.json(ApiResponse.success(result));
        });

        app.post("/api/cache/delete", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String id = RequestParser.requireField(body, "id");
            boolean deleted = CheckpointCache.getInstance().delete(id);
            ctx.json(ApiResponse.success(deleted));
        });
    }

    private static int parseQueryInt(String raw, int defaultValue, int minValue) {
        if (raw == null || raw.isEmpty()) return defaultValue;
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value: " + raw);
        }
        if (value < minValue || value > RequestParser.MAX_LIMIT) {
            throw new IllegalArgumentException(
                "Value must be between " + minValue + " and " + RequestParser.MAX_LIMIT);
        }
        return value;
    }
}
