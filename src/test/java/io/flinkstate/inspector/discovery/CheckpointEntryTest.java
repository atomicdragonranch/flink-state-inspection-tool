package io.flinkstate.inspector.discovery;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointEntryTest {

    @Test
    void shortJobIdTruncatesLongId() {
        // Arrange
        CheckpointEntry entry = new CheckpointEntry(
            "/tmp/chk-1", 0L, SnapshotType.CHECKPOINT, "abcdef1234567890");

        // Act / Assert
        assertThat(entry.shortJobId()).isEqualTo("abcdef12");
    }

    @Test
    void shortJobIdKeepsShortId() {
        // Arrange
        CheckpointEntry entry = new CheckpointEntry(
            "/tmp/chk-1", 0L, SnapshotType.CHECKPOINT, "abc");

        // Act / Assert
        assertThat(entry.shortJobId()).isEqualTo("abc");
    }

    @Test
    void shortJobIdReturnsNullForNullJobId() {
        // Arrange
        CheckpointEntry entry = new CheckpointEntry(
            "/tmp/chk-1", 0L, SnapshotType.SAVEPOINT, null);

        // Act / Assert
        assertThat(entry.shortJobId()).isNull();
    }

    @Test
    void displayNameExtractsLastSegment() {
        // Arrange
        CheckpointEntry entry = new CheckpointEntry(
            "/data/flink/abcdef1234567890/chk-42", 0L, SnapshotType.CHECKPOINT, "abcdef1234567890");

        // Act / Assert
        assertThat(entry.displayName()).isEqualTo("abcdef12/chk-42");
    }

    @Test
    void displayNameWithoutJobId() {
        // Arrange
        CheckpointEntry entry = new CheckpointEntry(
            "/data/flink/savepoint-abc123", 0L, SnapshotType.SAVEPOINT, null);

        // Act / Assert
        assertThat(entry.displayName()).isEqualTo("savepoint-abc123");
    }

    @Test
    void formattedTimeReturnsIsoInstant() {
        // Arrange
        CheckpointEntry entry = new CheckpointEntry(
            "/tmp/chk-1", 1718000000000L, SnapshotType.CHECKPOINT, "job1");

        // Act
        String formatted = entry.formattedTime();

        // Assert
        assertThat(formatted).startsWith("2024-06-10");
        assertThat(formatted).contains("T");
    }
}
