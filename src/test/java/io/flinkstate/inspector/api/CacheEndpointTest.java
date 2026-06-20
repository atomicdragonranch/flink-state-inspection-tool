package io.flinkstate.inspector.api;

import io.flinkstate.inspector.storage.CheckpointCache;
import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CacheEndpointTest {

    @BeforeEach
    void setUp() {
        CheckpointCache.getInstance().clear();
    }

    private Javalin createApp() {
        Javalin app = Javalin.create();
        CacheEndpoint.register(app);
        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400).json(ApiResponse.error(e.getMessage()));
        });
        return app;
    }

    @Test
    void listReturnsEmptyWhenNoCachedEntries() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/cache/list");

            assertThat(response.code()).isEqualTo(200);
            String body = response.body().string();
            assertThat(body).contains("\"totalCount\":0");
            assertThat(body).contains("\"entries\":[]");
        });
    }

    @Test
    void listReturnsCachedEntriesWithoutLocalPath(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-1");
        Files.createDirectories(chkDir);
        Files.writeString(chkDir.resolve("_metadata"), "data");
        String id = CheckpointCache.getInstance().register("docker://container/chk-1", chkDir.toString());

        // Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/cache/list");

            assertThat(response.code()).isEqualTo(200);
            String body = response.body().string();
            assertThat(body).contains("\"id\":\"" + id + "\"");
            assertThat(body).contains("\"sourcePath\":\"docker://container/chk-1\"");
            assertThat(body).contains("\"totalCount\":1");
            assertThat(body).doesNotContain("localPath");
        });
    }

    @Test
    void deleteByIdRemovesEntry(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-del");
        Files.createDirectories(chkDir);
        Files.writeString(chkDir.resolve("_metadata"), "data");
        String id = CheckpointCache.getInstance().register("source", chkDir.toString());

        // Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/cache/delete",
                "{\"id\": \"" + id + "\"}");

            assertThat(response.code()).isEqualTo(200);
            assertThat(CheckpointCache.getInstance().size()).isEqualTo(0);
        });
    }

    @Test
    void deleteReturns400WhenIdMissing() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/cache/delete", "{}");

            assertThat(response.code()).isEqualTo(400);
            assertThat(response.body().string()).contains("Missing required field: id");
        });
    }

    @Test
    void deleteReturnsFalseForUnknownId() {
        // Arrange / Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.post("/api/cache/delete",
                "{\"id\": \"nonexistent\"}");

            assertThat(response.code()).isEqualTo(200);
            assertThat(response.body().string()).contains("false");
        });
    }

    @Test
    void listRespectsLimitParameter(@TempDir Path tempDir) throws IOException {
        // Arrange
        for (int i = 0; i < 3; i++) {
            Path chkDir = tempDir.resolve("chk-" + i);
            Files.createDirectories(chkDir);
            Files.writeString(chkDir.resolve("_metadata"), "data");
            CheckpointCache.getInstance().register("source-" + i, chkDir.toString());
        }

        // Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/cache/list?limit=2");

            assertThat(response.code()).isEqualTo(200);
            String body = response.body().string();
            assertThat(body).contains("\"totalCount\":3");
        });
    }

    @Test
    void listRespectsOffsetParameter(@TempDir Path tempDir) throws IOException {
        // Arrange
        for (int i = 0; i < 3; i++) {
            Path chkDir = tempDir.resolve("chk-" + i);
            Files.createDirectories(chkDir);
            Files.writeString(chkDir.resolve("_metadata"), "data");
            CheckpointCache.getInstance().register("source-" + i, chkDir.toString());
        }

        // Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/cache/list?offset=2&limit=10");

            assertThat(response.code()).isEqualTo(200);
            String body = response.body().string();
            assertThat(body).contains("\"totalCount\":3");
            assertThat(body).contains("\"offset\":2");
        });
    }

    @Test
    void listReturnsDefaultOffsetZero(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-0");
        Files.createDirectories(chkDir);
        Files.writeString(chkDir.resolve("_metadata"), "data");
        CheckpointCache.getInstance().register("source-0", chkDir.toString());

        // Act / Assert
        JavalinTest.test(createApp(), (server, client) -> {
            var response = client.get("/api/cache/list");

            assertThat(response.code()).isEqualTo(200);
            String body = response.body().string();
            assertThat(body).contains("\"offset\":0");
        });
    }
}
