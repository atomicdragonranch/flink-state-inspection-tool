package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DiscoveryEndpointTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Javalin createApp() {
        Javalin app = Javalin.create();
        DiscoveryEndpoint.register(app);
        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(ApiResponse.error(e.getMessage()));
        });
        return app;
    }

    private JsonNode json(String raw) throws Exception {
        return MAPPER.readTree(raw);
    }

    @Test
    void discoverCheckpointsReturns400WhenPathMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/checkpoints/discover", "{}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void discoverCheckpointsReturns400WhenPathEmpty() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/checkpoints/discover",
                "{\"path\": \"\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void discoverSavepointsReturns400WhenPathMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/savepoints/discover", "{}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void discoverSavepointsReturns400WhenPathEmpty() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/savepoints/discover",
                "{\"path\": \"\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void discoverCheckpointsReturns400WhenTotalLimitInvalid() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/checkpoints/discover",
                "{\"path\": \"/tmp/chk\", \"totalLimit\": 0}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("totalLimit must be between 1 and");
        });
    }

    @Test
    void extractConfigReturnsEmptyMapWhenConfigAbsent() throws Exception {
        // Arrange
        JsonNode body = json("{\"path\": \"/tmp/chk\"}");

        // Act
        Map<String, String> config = DiscoveryEndpoint.extractConfig(body);

        // Assert
        assertThat(config).isEmpty();
    }

    @Test
    void extractConfigReturnsEmptyMapWhenConfigIsNotObject() throws Exception {
        // Arrange
        JsonNode body = json("{\"path\": \"/tmp/chk\", \"config\": \"not-an-object\"}");

        // Act
        Map<String, String> config = DiscoveryEndpoint.extractConfig(body);

        // Assert
        assertThat(config).isEmpty();
    }

    @Test
    void extractConfigReturnsEmptyMapWhenConfigIsNull() throws Exception {
        // Arrange
        JsonNode body = json("{\"path\": \"/tmp/chk\", \"config\": null}");

        // Act
        Map<String, String> config = DiscoveryEndpoint.extractConfig(body);

        // Assert
        assertThat(config).isEmpty();
    }

    @Test
    void extractConfigParsesAllFields() throws Exception {
        // Arrange
        JsonNode body = json("{\"config\": {\"endpoint\": \"http://localhost:9000\", \"region\": \"us-east-1\"}}");

        // Act
        Map<String, String> config = DiscoveryEndpoint.extractConfig(body);

        // Assert
        assertThat(config).hasSize(2);
        assertThat(config.get("endpoint")).isEqualTo("http://localhost:9000");
        assertThat(config.get("region")).isEqualTo("us-east-1");
    }

    @Test
    void extractConfigConvertsNonStringValuesToText() throws Exception {
        // Arrange
        JsonNode body = json("{\"config\": {\"port\": 9000, \"enabled\": true}}");

        // Act
        Map<String, String> config = DiscoveryEndpoint.extractConfig(body);

        // Assert
        assertThat(config).hasSize(2);
        assertThat(config.get("port")).isEqualTo("9000");
        assertThat(config.get("enabled")).isEqualTo("true");
    }
}
