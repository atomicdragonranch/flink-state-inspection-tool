package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import java.util.Map;

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
}
