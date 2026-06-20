package io.flinkstate.inspector.api;

import io.flinkstate.inspector.reader.StateReadResult;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DiffEndpointTest {

    private Javalin createApp() {
        Javalin app = Javalin.create();
        DiffEndpoint.register(app);
        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(ApiResponse.error(e.getMessage()));
        });
        return app;
    }

    @Test
    void keyedDiffReturns400WhenPath1Missing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/diff/keyed",
                "{\"path2\": \"/b\", \"operatorUid\": \"abc\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("path1");
        });
    }

    @Test
    void keyedDiffReturns400WhenPath2Missing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/diff/keyed",
                "{\"path1\": \"/a\", \"operatorUid\": \"abc\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("path2");
        });
    }

    @Test
    void keyedDiffReturns400WhenOperatorUidMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/diff/keyed",
                "{\"path1\": \"/a\", \"path2\": \"/b\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("operatorUid");
        });
    }

    @Test
    void broadcastDiffReturns400WhenFieldsMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/diff/broadcast", "{}");

            assertThat(response.code()).isEqualTo(400);
        });
    }

    @Test
    void computeDiffDetectsAddedKeys() {
        // Arrange
        List<Map<String, Object>> entries1 = new ArrayList<>();
        entries1.add(entry("sensor-0", 100.0));

        List<Map<String, Object>> entries2 = new ArrayList<>();
        entries2.add(entry("sensor-0", 100.0));
        entries2.add(entry("sensor-1", 200.0));

        StateReadResult result1 = new StateReadResult("op", entries1, List.of("key", "value"));
        StateReadResult result2 = new StateReadResult("op", entries2, List.of("key", "value"));

        // Act
        Map<String, Object> diff = DiffEndpoint.computeDiff(
            result1, result2, "op", "/chk-1", "/chk-2");

        // Assert
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> added = (List<Map<String, Object>>) diff.get("added");
        assertThat(added).hasSize(1);
        assertThat(added.get(0).get("key")).isEqualTo("sensor-1");
        assertThat(diff.get("unchangedCount")).isEqualTo(1);
    }

    @Test
    void computeDiffDetectsRemovedKeys() {
        // Arrange
        List<Map<String, Object>> entries1 = new ArrayList<>();
        entries1.add(entry("sensor-0", 100.0));
        entries1.add(entry("sensor-1", 200.0));

        List<Map<String, Object>> entries2 = new ArrayList<>();
        entries2.add(entry("sensor-0", 100.0));

        StateReadResult result1 = new StateReadResult("op", entries1, List.of("key", "value"));
        StateReadResult result2 = new StateReadResult("op", entries2, List.of("key", "value"));

        // Act
        Map<String, Object> diff = DiffEndpoint.computeDiff(
            result1, result2, "op", "/chk-1", "/chk-2");

        // Assert
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> removed = (List<Map<String, Object>>) diff.get("removed");
        assertThat(removed).hasSize(1);
        assertThat(removed.get(0).get("key")).isEqualTo("sensor-1");
    }

    @Test
    void computeDiffDetectsModifiedValues() {
        // Arrange
        List<Map<String, Object>> entries1 = new ArrayList<>();
        entries1.add(entry("sensor-0", 100.0));

        List<Map<String, Object>> entries2 = new ArrayList<>();
        entries2.add(entry("sensor-0", 150.0));

        StateReadResult result1 = new StateReadResult("op", entries1, List.of("key", "value"));
        StateReadResult result2 = new StateReadResult("op", entries2, List.of("key", "value"));

        // Act
        Map<String, Object> diff = DiffEndpoint.computeDiff(
            result1, result2, "op", "/chk-1", "/chk-2");

        // Assert
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> modified = (List<Map<String, Object>>) diff.get("modified");
        assertThat(modified).hasSize(1);
        assertThat(modified.get(0).get("key")).isEqualTo("sensor-0");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fieldChanges =
            (List<Map<String, Object>>) modified.get(0).get("fieldChanges");
        assertThat(fieldChanges).isNotEmpty();
        assertThat(fieldChanges.get(0).get("fieldName")).isEqualTo("value");
    }

    @Test
    void computeDiffReportsUnchangedCount() {
        // Arrange
        List<Map<String, Object>> entries1 = new ArrayList<>();
        entries1.add(entry("sensor-0", 100.0));
        entries1.add(entry("sensor-1", 200.0));

        List<Map<String, Object>> entries2 = new ArrayList<>();
        entries2.add(entry("sensor-0", 100.0));
        entries2.add(entry("sensor-1", 200.0));

        StateReadResult result1 = new StateReadResult("op", entries1, List.of("key", "value"));
        StateReadResult result2 = new StateReadResult("op", entries2, List.of("key", "value"));

        // Act
        Map<String, Object> diff = DiffEndpoint.computeDiff(
            result1, result2, "op", "/chk-1", "/chk-2");

        // Assert
        assertThat(diff.get("unchangedCount")).isEqualTo(2);
        assertThat(diff.get("totalKeys")).isEqualTo(2);
        @SuppressWarnings("unchecked")
        List<?> added = (List<?>) diff.get("added");
        @SuppressWarnings("unchecked")
        List<?> removed = (List<?>) diff.get("removed");
        @SuppressWarnings("unchecked")
        List<?> modified = (List<?>) diff.get("modified");
        assertThat(added).isEmpty();
        assertThat(removed).isEmpty();
        assertThat(modified).isEmpty();
    }

    @Test
    void computeDiffHandlesMultipleStateColumns() {
        // Arrange
        Map<String, Object> e1 = new LinkedHashMap<>();
        e1.put("key", "sensor-0");
        e1.put("running-average", 85.0);
        e1.put("event-count", 100L);

        Map<String, Object> e2 = new LinkedHashMap<>();
        e2.put("key", "sensor-0");
        e2.put("running-average", 90.0);
        e2.put("event-count", 130L);

        StateReadResult result1 = new StateReadResult("op", List.of(e1),
            List.of("key", "running-average", "event-count"));
        StateReadResult result2 = new StateReadResult("op", List.of(e2),
            List.of("key", "running-average", "event-count"));

        // Act
        Map<String, Object> diff = DiffEndpoint.computeDiff(
            result1, result2, "op", "/chk-1", "/chk-2");

        // Assert
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> modified = (List<Map<String, Object>>) diff.get("modified");
        assertThat(modified).hasSize(1);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> fieldChanges =
            (List<Map<String, Object>>) modified.get(0).get("fieldChanges");
        assertThat(fieldChanges).hasSize(2);
        assertThat(fieldChanges).extracting("fieldName")
            .containsExactly("running-average", "event-count");
    }

    private static Map<String, Object> entry(String key, Object value) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("key", key);
        map.put("value", value);
        return map;
    }
}
