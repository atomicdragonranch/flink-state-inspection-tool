package io.flinkstate.inspector.api;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

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
    void keyedDiffReturnsStubResponse() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/diff/keyed",
                "{\"path1\": \"/a\", \"path2\": \"/b\", \"operatorUid\": \"abc\"}");

            assertThat(response.code()).isEqualTo(200);
            String body = response.body().string();
            assertThat(body).contains("\"label1\":\"a\"");
            assertThat(body).contains("\"label2\":\"b\"");
            assertThat(body).contains("\"stub\":true");
            assertThat(body).contains("\"partialRead\":true");
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
}
