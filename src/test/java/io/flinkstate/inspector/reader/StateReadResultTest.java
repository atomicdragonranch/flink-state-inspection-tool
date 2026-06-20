package io.flinkstate.inspector.reader;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StateReadResultTest {

    @Test
    void gettersReturnConstructorValues() {
        // Arrange
        List<Map<String, Object>> entries = List.of(
            Map.of("key", "k1", "value", 42),
            Map.of("key", "k2", "value", 99)
        );
        List<String> columns = List.of("key", "value");

        // Act
        StateReadResult result = new StateReadResult("op-123", entries, columns);

        // Assert
        assertThat(result.getOperatorUid()).isEqualTo("op-123");
        assertThat(result.getEntries()).isEqualTo(entries);
        assertThat(result.getColumns()).isEqualTo(columns);
    }

    @Test
    void entryCountMatchesListSize() {
        // Arrange
        List<Map<String, Object>> entries = List.of(
            Map.of("key", "a"),
            Map.of("key", "b"),
            Map.of("key", "c")
        );

        // Act
        StateReadResult result = new StateReadResult("op-1", entries, List.of("key"));

        // Assert
        assertThat(result.getEntryCount()).isEqualTo(3);
    }

    @Test
    void emptyEntriesReturnsZeroCount() {
        // Arrange / Act
        StateReadResult result = new StateReadResult("op-1", List.of(), List.of("key"));

        // Assert
        assertThat(result.getEntryCount()).isEqualTo(0);
    }

    @Test
    void defaultConstructorHasZeroSkippedFilesAndNoWarnings() {
        // Arrange / Act
        StateReadResult result = new StateReadResult("op-1", List.of(), List.of("key"));

        // Assert
        assertThat(result.getSkippedSstFiles()).isEqualTo(0);
        assertThat(result.getWarnings()).isEmpty();
    }

    @Test
    void fullConstructorTracksSkippedFiles() {
        // Arrange
        List<String> warnings = List.of("2 SST file(s) skipped: FRocksDB format");

        // Act
        StateReadResult result = new StateReadResult(
            "op-1", List.of(), List.of("key"), 2, warnings);

        // Assert
        assertThat(result.getSkippedSstFiles()).isEqualTo(2);
        assertThat(result.getWarnings()).containsExactly("2 SST file(s) skipped: FRocksDB format");
    }

    @Test
    void warningsListIsImmutable() {
        // Arrange
        List<String> warnings = new java.util.ArrayList<>();
        warnings.add("test warning");
        StateReadResult result = new StateReadResult(
            "op-1", List.of(), List.of("key"), 1, warnings);

        // Act - modify the original list
        warnings.add("another warning");

        // Assert - result should not be affected
        assertThat(result.getWarnings()).hasSize(1);
        assertThat(result.getWarnings()).containsExactly("test warning");
    }

    @Test
    void nullWarningsDefaultsToEmptyList() {
        // Arrange / Act
        StateReadResult result = new StateReadResult(
            "op-1", List.of(), List.of("key"), 0, null);

        // Assert
        assertThat(result.getWarnings()).isEmpty();
    }
}
