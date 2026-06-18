package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StorageConnectorFactoryTest {

    @Test
    void localPathReturnsLocalConnector() {
        // Arrange
        String path = "/tmp/flink/checkpoints";

        // Act
        StorageConnector connector = StorageConnectorFactory.create(path);

        // Assert
        assertThat(connector).isInstanceOf(LocalStorageConnector.class);
        assertThat(connector.scheme()).isEqualTo("file");
    }

    @Test
    void s3PathReturnsS3Connector() {
        // Arrange
        String path = "s3://my-bucket/flink/checkpoints";

        // Act / Assert
        assertThat(resolveType(path)).isEqualTo(S3StorageConnector.class);
    }

    @Test
    void s3aPathReturnsS3Connector() {
        // Arrange
        String path = "s3a://my-bucket/flink/checkpoints";

        // Act / Assert
        assertThat(resolveType(path)).isEqualTo(S3StorageConnector.class);
    }

    @Test
    void gcsPathReturnsGcsConnector() {
        // Arrange
        String path = "gs://my-bucket/flink/checkpoints";

        // Act / Assert
        assertThat(resolveType(path)).isEqualTo(GcsStorageConnector.class);
    }

    @Test
    void dockerPathReturnsDockerConnector() {
        // Arrange
        String path = "docker://flink-taskmanager/opt/flink/checkpoints";

        // Act / Assert
        assertThat(resolveType(path)).isEqualTo(DockerStorageConnector.class);
    }

    @Test
    void windowsPathReturnsLocalConnector() {
        // Arrange
        String path = "C:\\flink\\checkpoints";

        // Act / Assert
        assertThat(resolveType(path)).isEqualTo(LocalStorageConnector.class);
    }

    private Class<?> resolveType(String path) {
        return StorageConnectorFactory.create(path).getClass();
    }
}
