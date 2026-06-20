package io.flinkstate.inspector.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointCacheTest {

    private CheckpointCache cache;

    @BeforeEach
    void setUp() {
        cache = CheckpointCache.getInstance();
        cache.clear();
    }

    @Test
    void registerAddsEntry(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-1");
        Files.createDirectories(chkDir);
        Files.writeString(chkDir.resolve("_metadata"), "test-data");

        // Act
        String id = cache.register("docker://container/path/chk-1", chkDir.toString());

        // Assert
        assertThat(id).isNotNull().isNotEmpty();
        assertThat(cache.size()).isEqualTo(1);
        List<Map<String, Object>> entries = cache.listEntries();
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).get("id")).isEqualTo(id);
        assertThat(entries.get(0).get("sourcePath")).isEqualTo("docker://container/path/chk-1");
        assertThat(entries.get(0)).doesNotContainKey("localPath");
        assertThat((long) entries.get(0).get("sizeBytes")).isGreaterThan(0);
        assertThat((long) entries.get(0).get("cachedAt")).isGreaterThan(0);
    }

    @Test
    void deleteByIdRemovesEntryAndFiles(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-to-delete");
        Files.createDirectories(chkDir);
        Files.writeString(chkDir.resolve("_metadata"), "test-data");
        Files.writeString(chkDir.resolve("000001.sst"), "sst-data");
        String id = cache.register("docker://container/chk-1", chkDir.toString());

        // Act
        boolean deleted = cache.delete(id);

        // Assert
        assertThat(deleted).isTrue();
        assertThat(cache.size()).isEqualTo(0);
        assertThat(chkDir.toFile().exists()).isFalse();
    }

    @Test
    void deleteReturnsFalseForUnknownId() {
        // Arrange / Act
        boolean deleted = cache.delete("nonexistent-id");

        // Assert
        assertThat(deleted).isFalse();
    }

    @Test
    void listEntriesReturnsNewestFirst(@TempDir Path tempDir) throws Exception {
        // Arrange
        Path chk1 = tempDir.resolve("chk-1");
        Path chk2 = tempDir.resolve("chk-2");
        Files.createDirectories(chk1);
        Files.createDirectories(chk2);
        cache.register("source-1", chk1.toString());
        Thread.sleep(10);
        cache.register("source-2", chk2.toString());

        // Act
        List<Map<String, Object>> entries = cache.listEntries();

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).get("sourcePath")).isEqualTo("source-2");
        assertThat(entries.get(1).get("sourcePath")).isEqualTo("source-1");
    }

    @Test
    void listEntriesDoesNotExposeLocalPath(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-1");
        Files.createDirectories(chkDir);
        cache.register("source-1", chkDir.toString());

        // Act
        List<Map<String, Object>> entries = cache.listEntries();

        // Assert
        assertThat(entries.get(0)).doesNotContainKey("localPath");
    }

    @Test
    void lookupLocalPathFindsRegisteredCheckpoint(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chkDir = tempDir.resolve("chk-1");
        Files.createDirectories(chkDir);
        cache.register("docker://container/chk-1", chkDir.toString());

        // Act
        String localPath = cache.lookupLocalPath("docker://container/chk-1");

        // Assert
        assertThat(localPath).isEqualTo(chkDir.toString());
    }

    @Test
    void clearRemovesAllEntries(@TempDir Path tempDir) throws IOException {
        // Arrange
        Path chk1 = tempDir.resolve("chk-1");
        Path chk2 = tempDir.resolve("chk-2");
        Files.createDirectories(chk1);
        Files.createDirectories(chk2);
        cache.register("source-1", chk1.toString());
        cache.register("source-2", chk2.toString());

        // Act
        cache.clear();

        // Assert
        assertThat(cache.size()).isEqualTo(0);
        assertThat(cache.listEntries()).isEmpty();
    }
}
