package io.flinkstate.inspector.api;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InspectEndpointTest {

    private Javalin createApp() {
        Javalin app = Javalin.create();
        InspectEndpoint.register(app);
        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(ApiResponse.error(e.getMessage()));
        });
        return app;
    }

    @Test
    void keyedInspectReturns400WhenPathMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/keyed",
                "{\"operatorUid\": \"op1\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void keyedInspectReturns400WhenOperatorUidMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/keyed",
                "{\"path\": \"/tmp/chk\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: operatorUid");
        });
    }

    @Test
    void keyedInspectReturns400WhenBodyEmpty() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/keyed", "{}");

            assertThat(response.code()).isEqualTo(400);
        });
    }

    @Test
    void keyedInspectReturns400WhenLimitInvalid() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/keyed",
                "{\"path\": \"/tmp/chk\", \"operatorUid\": \"op1\", \"limit\": 0}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("limit must be between 1 and");
        });
    }

    @Test
    void broadcastInspectReturns400WhenPathMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/broadcast",
                "{\"operatorUid\": \"op1\", \"stateName\": \"state1\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void broadcastInspectReturns400WhenOperatorUidMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/broadcast",
                "{\"path\": \"/tmp/chk\", \"stateName\": \"state1\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: operatorUid");
        });
    }

    @Test
    void broadcastInspectReturns400WhenStateNameMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/broadcast",
                "{\"path\": \"/tmp/chk\", \"operatorUid\": \"op1\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: stateName");
        });
    }

    @Test
    void broadcastInspectReturns400WhenBodyEmpty() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/broadcast", "{}");

            assertThat(response.code()).isEqualTo(400);
        });
    }

    @Test
    void broadcastInspectReturns400WhenLimitInvalid() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/inspect/broadcast",
                "{\"path\": \"/tmp/chk\", \"operatorUid\": \"op1\", \"stateName\": \"s\", \"limit\": -5}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("limit must be between 1 and");
        });
    }
}
