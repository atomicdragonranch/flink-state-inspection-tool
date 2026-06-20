package io.flinkstate.inspector.api;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorEndpointTest {

    private Javalin createApp() {
        Javalin app = Javalin.create();
        OperatorEndpoint.register(app);
        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(ApiResponse.error(e.getMessage()));
        });
        return app;
    }

    @Test
    void discoverReturns400WhenPathMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/operators/discover", "{}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void discoverReturns400WhenPathEmpty() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/operators/discover",
                "{\"path\": \"\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }

    @Test
    void discoverReturns400WhenBodyHasNoPath() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/operators/discover",
                "{\"other\": \"value\"}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: path");
        });
    }
}
