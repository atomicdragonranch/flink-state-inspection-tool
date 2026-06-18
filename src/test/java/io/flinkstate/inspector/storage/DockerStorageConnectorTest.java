package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DockerStorageConnectorTest {

    @Test
    void parseDockerUriWithContainerAndPath() {
        // Arrange
        String uri = "docker://flink-taskmanager/opt/flink/checkpoints";

        // Act
        String[] result = DockerStorageConnector.parseDockerUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("flink-taskmanager");
        assertThat(result[1]).isEqualTo("/opt/flink/checkpoints");
    }

    @Test
    void parseDockerUriWithContainerOnly() {
        // Arrange
        String uri = "docker://flink-taskmanager";

        // Act
        String[] result = DockerStorageConnector.parseDockerUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("flink-taskmanager");
        assertThat(result[1]).isEqualTo("/");
    }

    @Test
    void parseDockerUriWithNestedPath() {
        // Arrange
        String uri = "docker://my-container/var/data/flink/chk-123";

        // Act
        String[] result = DockerStorageConnector.parseDockerUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my-container");
        assertThat(result[1]).isEqualTo("/var/data/flink/chk-123");
    }
}
