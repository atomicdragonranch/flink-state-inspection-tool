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

import java.util.LinkedHashMap;
import java.util.Map;

public final class InspectEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(InspectEndpoint.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_LIMIT = 1000;

    private InspectEndpoint() {
    }

    public static void register(Javalin app) {
        app.post("/api/inspect/keyed", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path = requireField(body, "path");
            String operatorUid = requireField(body, "operatorUid");
            String keyFilter = optionalField(body, "keyFilter");
            boolean keysOnly = boolField(body, "keysOnly");
            int limit = intField(body, "limit", DEFAULT_LIMIT);

            LOG.info("Inspect keyed: path={}, operator={}", path, operatorUid);

            String cachedPath = CheckpointCache.getInstance().lookupLocalPath(path);
            Map<String, String> connectorConfig = DiscoveryEndpoint.extractConfig(body);
            try (StorageConnector connector = StorageConnectorFactory.create(path, connectorConfig)) {
                String localPath = cachedPath != null ? cachedPath
                    : connector.resolveFullCheckpoint(path);
                StateReadResult result = GenericStateReader.readKeyedState(
                    localPath, operatorUid, keyFilter, keysOnly, limit);

                Map<String, Object> data = new LinkedHashMap<>();
                data.put("operatorUid", result.getOperatorUid());
                data.put("entryCount", result.getEntryCount());
                data.put("keysOnly", keysOnly);
                data.put("columns", result.getColumns());
                data.put("entries", result.getEntries());
                ctx.json(ApiResponse.success(data));
            } catch (Exception e) {
                LOG.error("Failed to read keyed state", e);
                String rootCause = e.getMessage();
                Throwable cause = e.getCause();
                while (cause != null) {
                    rootCause = cause.getClass().getSimpleName() + ": " + cause.getMessage();
                    cause = cause.getCause();
                }
                ctx.status(500);
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("error", rootCause);
                java.io.StringWriter sw = new java.io.StringWriter();
                e.printStackTrace(new java.io.PrintWriter(sw));
                err.put("stackTrace", sw.toString());
                ctx.json(err);
            }
        });

        app.post("/api/inspect/broadcast", ctx -> {
            JsonNode body = MAPPER.readTree(ctx.body());
            String path = requireField(body, "path");
            String operatorUid = requireField(body, "operatorUid");
            String stateName = requireField(body, "stateName");
            String keyFilter = optionalField(body, "keyFilter");
            int limit = intField(body, "limit", DEFAULT_LIMIT);

            LOG.info("Inspect operator state: path={}, operator={}, state={}",
                path, operatorUid, stateName);

            String cachedPath = CheckpointCache.getInstance().lookupLocalPath(path);
            Map<String, String> connectorConfig = DiscoveryEndpoint.extractConfig(body);
            try (StorageConnector connector = StorageConnectorFactory.create(path, connectorConfig)) {
                String localPath = cachedPath != null ? cachedPath
                    : connector.resolveFullCheckpoint(path);
                StateReadResult result = OperatorStateReader.readOperatorState(
                    localPath, operatorUid, stateName, keyFilter, limit);

                Map<String, Object> data = new LinkedHashMap<>();
                data.put("operatorUid", result.getOperatorUid());
                data.put("stateName", stateName);
                data.put("entryCount", result.getEntryCount());
                data.put("columns", result.getColumns());
                data.put("entries", result.getEntries());
                ctx.json(ApiResponse.success(data));
            } catch (Exception e) {
                LOG.error("Failed to read operator state", e);
                String rootCause = e.getMessage();
                Throwable cause = e.getCause();
                while (cause != null) {
                    rootCause = cause.getClass().getSimpleName() + ": " + cause.getMessage();
                    cause = cause.getCause();
                }
                ctx.status(500);
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("error", rootCause);
                java.io.StringWriter sw = new java.io.StringWriter();
                e.printStackTrace(new java.io.PrintWriter(sw));
                err.put("stackTrace", sw.toString());
                ctx.json(err);
            }
        });
    }

    private static String requireField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.asText().isEmpty()) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return node.asText();
    }

    private static String optionalField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.isNull() || node.asText().isEmpty()) {
            return null;
        }
        return node.asText();
    }

    private static boolean boolField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        return node != null && node.asBoolean();
    }

    private static int intField(JsonNode body, String fieldName, int defaultValue) {
        JsonNode node = body.get(fieldName);
        return node != null && node.isNumber() ? node.asInt() : defaultValue;
    }
}
