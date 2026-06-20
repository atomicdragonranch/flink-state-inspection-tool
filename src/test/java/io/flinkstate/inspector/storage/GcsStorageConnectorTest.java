package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class GcsStorageConnectorTest {

    @Test
    void parseGcsUriWithBucketAndKey() {
        // Arrange
        String uri = "gs://my-bucket/flink/checkpoints";

        // Act
        String[] result = GcsStorageConnector.parseGcsUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEqualTo("flink/checkpoints");
    }

    @Test
    void parseGcsUriWithBucketOnly() {
        // Arrange
        String uri = "gs://my-bucket";

        // Act
        String[] result = GcsStorageConnector.parseGcsUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEmpty();
    }

    @Test
    void parseGcsUriStripsTrailingSlash() {
        // Arrange
        String uri = "gs://my-bucket/checkpoints/";

        // Act
        String[] result = GcsStorageConnector.parseGcsUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEqualTo("checkpoints");
    }

    @Test
    void parseGcsUriWithDeeplyNestedKey() {
        // Arrange
        String uri = "gs://prod-bucket/flink/jobs/abc123/chk-42";

        // Act
        String[] result = GcsStorageConnector.parseGcsUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("prod-bucket");
        assertThat(result[1]).isEqualTo("flink/jobs/abc123/chk-42");
    }

    @Test
    void parseGcsUriWithBucketAndTrailingSlashOnly() {
        // Arrange
        String uri = "gs://my-bucket/";

        // Act
        String[] result = GcsStorageConnector.parseGcsUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEmpty();
    }

    @Test
    void schemeReturnsGs() {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();

        // Act
        String scheme = connector.scheme();

        // Assert
        assertThat(scheme).isEqualTo("gs");
    }

    @Test
    void closeWithNullClientDoesNotThrow() {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();

        // Act / Assert
        connector.close();
    }

    @Test
    void ensureTempDirCreatesTempDirectory() throws IOException {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();

        // Act
        connector.ensureTempDir();

        // Assert
        Path tempDir = connector.getTempDirRef().get();
        assertThat(tempDir).isNotNull();
        assertThat(Files.exists(tempDir)).isTrue();
        assertThat(Files.isDirectory(tempDir)).isTrue();

        // Cleanup
        Files.deleteIfExists(tempDir);
    }

    @Test
    void ensureTempDirIsIdempotent() throws IOException {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();

        // Act
        connector.ensureTempDir();
        Path firstDir = connector.getTempDirRef().get();
        connector.ensureTempDir();
        Path secondDir = connector.getTempDirRef().get();

        // Assert
        assertThat(secondDir).isEqualTo(firstDir);

        // Cleanup
        Files.deleteIfExists(firstDir);
    }

    @Test
    void ensureTempDirIsThreadSafe() throws Exception {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Path>> futures = new ArrayList<>();

        // Act
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                startLatch.await();
                connector.ensureTempDir();
                return connector.getTempDirRef().get();
            }));
        }
        startLatch.countDown();

        List<Path> results = new ArrayList<>();
        for (Future<Path> f : futures) {
            results.add(f.get());
        }
        executor.shutdown();

        // Assert
        Path expected = results.get(0);
        assertThat(expected).isNotNull();
        for (Path p : results) {
            assertThat(p).isEqualTo(expected);
        }

        // Cleanup
        Files.deleteIfExists(expected);
    }

    @Test
    void closeCleansTempDir() throws IOException {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();
        connector.ensureTempDir();
        Path tempDir = connector.getTempDirRef().get();
        Path subFile = tempDir.resolve("test-file.txt");
        Files.writeString(subFile, "test content");

        // Act
        connector.close();

        // Assert
        assertThat(Files.exists(tempDir)).isFalse();
        assertThat(Files.exists(subFile)).isFalse();
    }

    @Test
    void tempDirRefStartsNull() {
        // Arrange
        GcsStorageConnector connector = new GcsStorageConnector();

        // Act
        Path tempDir = connector.getTempDirRef().get();

        // Assert
        assertThat(tempDir).isNull();
    }
}
