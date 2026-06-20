package io.flinkstate.inspector.api;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DocsEndpointTest {

    private Javalin createApp() {
        Javalin app = Javalin.create();
        DocsEndpoint.register(app);
        return app;
    }

    @Test
    void rejectsPathTraversalWithDotDot() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/docs/..%2F..%2Fetc%2Fpasswd");

            assertThat(response.code()).isEqualTo(400);
        });
    }

    @Test
    void rejectsPathWithSlash() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/docs/foo%2Fbar");

            assertThat(response.code()).isEqualTo(400);
        });
    }

    @Test
    void returns404ForNonexistentDoc() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/docs/nonexistent");

            assertThat(response.code()).isEqualTo(404);
        });
    }

    @Test
    void listsAvailableDocs() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/docs");

            assertThat(response.code()).isEqualTo(200);
            assertThat(response.body().string()).contains("\"data\":");
        });
    }
}
