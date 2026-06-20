package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class S3StorageConnectorTest {

    @Test
    void parseS3UriWithBucketAndKey() {
        // Arrange
        String uri = "s3://my-bucket/flink/checkpoints";

        // Act
        String[] result = S3StorageConnector.parseS3Uri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEqualTo("flink/checkpoints");
    }

    @Test
    void parseS3UriWithBucketOnly() {
        // Arrange
        String uri = "s3://my-bucket";

        // Act
        String[] result = S3StorageConnector.parseS3Uri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEmpty();
    }

    @Test
    void parseS3aScheme() {
        // Arrange
        String uri = "s3a://hadoop-bucket/data/checkpoints";

        // Act
        String[] result = S3StorageConnector.parseS3Uri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("hadoop-bucket");
        assertThat(result[1]).isEqualTo("data/checkpoints");
    }

    @Test
    void parseS3UriStripsTrailingSlash() {
        // Arrange
        String uri = "s3://my-bucket/checkpoints/";

        // Act
        String[] result = S3StorageConnector.parseS3Uri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEqualTo("checkpoints");
    }

    @Test
    void parseS3UriWithDeeplyNestedKey() {
        // Arrange
        String uri = "s3://prod-bucket/flink/jobs/abc123/chk-42";

        // Act
        String[] result = S3StorageConnector.parseS3Uri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("prod-bucket");
        assertThat(result[1]).isEqualTo("flink/jobs/abc123/chk-42");
    }

    @Test
    void parseS3UriWithBucketAndTrailingSlashOnly() {
        // Arrange
        String uri = "s3://my-bucket/";

        // Act
        String[] result = S3StorageConnector.parseS3Uri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-bucket");
        assertThat(result[1]).isEmpty();
    }

    @Test
    void initializeCreatesS3Client() {
        // Arrange
        S3StorageConnector connector = new S3StorageConnector();
        Map<String, String> config = Map.of(
            "aws.region", "us-west-2",
            "aws.access-key-id", "test-key",
            "aws.secret-access-key", "test-secret"
        );

        // Act
        connector.initialize(config);

        // Assert
        assertThat(connector.getS3Client()).isNotNull();
        connector.close();
    }

    @Test
    void initializeWithEndpointAndPathStyle() {
        // Arrange
        S3StorageConnector connector = new S3StorageConnector();
        Map<String, String> config = Map.of(
            "aws.region", "us-east-1",
            "aws.endpoint", "http://localhost:4566",
            "aws.path-style-access", "true",
            "aws.access-key-id", "test",
            "aws.secret-access-key", "test"
        );

        // Act
        connector.initialize(config);

        // Assert
        assertThat(connector.getS3Client()).isNotNull();
        connector.close();
    }

    @Test
    void closeWithNullClientDoesNotThrow() {
        // Arrange
        S3StorageConnector connector = new S3StorageConnector();

        // Act / Assert
        connector.close();
    }

    @Test
    void ensureTempDirCreatesTempDirectory() throws IOException {
        // Arrange
        S3StorageConnector connector = new S3StorageConnector();

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
        S3StorageConnector connector = new S3StorageConnector();

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
        S3StorageConnector connector = new S3StorageConnector();
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
        S3StorageConnector connector = new S3StorageConnector();
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
}
