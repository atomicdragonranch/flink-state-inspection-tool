package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalStorageConnectorTest {

    @Test
    void rejectsPathWithDotDotTraversal() {
        // Arrange / Act / Assert
        assertThatThrownBy(() -> LocalStorageConnector.validatePath("/tmp/../etc/passwd"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("..");
    }

    @Test
    void rejectsNullPath() {
        // Arrange / Act / Assert
        assertThatThrownBy(() -> LocalStorageConnector.validatePath(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("required");
    }

    @Test
    void rejectsEmptyPath() {
        // Arrange / Act / Assert
        assertThatThrownBy(() -> LocalStorageConnector.validatePath(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("required");
    }

    @Test
    void acceptsValidAbsolutePath() {
        // Arrange / Act / Assert
        LocalStorageConnector.validatePath(System.getProperty("java.io.tmpdir"));
    }

    @Test
    void schemeReturnsFile() {
        // Arrange
        LocalStorageConnector connector = new LocalStorageConnector();

        // Act / Assert
        assertThat(connector.scheme()).isEqualTo("file");
    }

    @Test
    void discoverReturnsEmptyForNonexistentDir() {
        // Arrange
        LocalStorageConnector connector = new LocalStorageConnector();

        // Act
        var results = connector.discoverCheckpoints(
            System.getProperty("java.io.tmpdir") + "/nonexistent-dir-12345", 10);

        // Assert
        assertThat(results).isEmpty();
    }
}
