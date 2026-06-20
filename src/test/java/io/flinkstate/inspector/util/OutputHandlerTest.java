package io.flinkstate.inspector.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutputHandlerTest {

    @Test
    void toJsonProducesPrettyPrintedOutput() {
        // Arrange
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("name", "test");
        data.put("count", 42);

        // Act
        String result = OutputHandler.toJson(data);

        // Assert
        assertThat(result).contains("\"name\" : \"test\"");
        assertThat(result).contains("\"count\" : 42");
        assertThat(result).contains("\n"); // pretty-printed = has newlines
    }

    @Test
    void toJsonSerializesList() {
        // Arrange
        List<Map<String, String>> data = List.of(
            Map.of("key", "value1"),
            Map.of("key", "value2")
        );

        // Act
        String result = OutputHandler.toJson(data);

        // Assert
        assertThat(result).startsWith("[");
        assertThat(result).contains("\"value1\"");
        assertThat(result).contains("\"value2\"");
    }

    @Test
    void writeToFileCreatesFileWithContent(@TempDir Path tempDir) throws IOException {
        // Arrange
        String content = "test output content";
        Path filePath = tempDir.resolve("output.txt");

        // Act
        OutputHandler.writeToFile(content, filePath.toString());

        // Assert
        assertThat(filePath).exists();
        assertThat(filePath).hasContent(content);
    }

    @Test
    void writeToFileCreatesParentDirectories(@TempDir Path tempDir) throws IOException {
        // Arrange
        String content = "nested output";
        Path filePath = tempDir.resolve("sub").resolve("dir").resolve("output.txt");

        // Act
        OutputHandler.writeToFile(content, filePath.toString());

        // Assert
        assertThat(filePath).exists();
        assertThat(filePath).hasContent(content);
    }

    @Test
    void writeInJsonModeOutputsJson() {
        // Arrange
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("status", "ok");
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try {
            // Act
            OutputHandler.write(data, List.of("H"), Collections.emptyList(), null, true, null);

            // Assert
            String output = baos.toString();
            assertThat(output).contains("\"status\" : \"ok\"");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void writeInTableModeOutputsFormattedTable() {
        // Arrange
        List<String> headers = List.of("COL1", "COL2");
        List<List<String>> rows = List.of(List.of("a", "b"));
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try {
            // Act
            OutputHandler.write(null, headers, rows, "1 row", false, null);

            // Assert
            String output = baos.toString();
            assertThat(output).contains("COL1");
            assertThat(output).contains("COL2");
            assertThat(output).contains("1 row");
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    void writeToFileInTableMode(@TempDir Path tempDir) {
        // Arrange
        List<String> headers = List.of("NAME", "VALUE");
        List<List<String>> rows = List.of(List.of("key1", "val1"));
        Path outputPath = tempDir.resolve("table-output.txt");

        // Act
        OutputHandler.write(null, headers, rows, "1 entry", false, outputPath.toString());

        // Assert
        assertThat(outputPath).exists();
        try {
            String fileContent = Files.readString(outputPath);
            assertThat(fileContent).contains("NAME");
            assertThat(fileContent).contains("VALUE");
            assertThat(fileContent).contains("key1");
            assertThat(fileContent).contains("1 entry");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void writeToFileInJsonMode(@TempDir Path tempDir) {
        // Arrange
        Map<String, Object> data = Map.of("result", "success");
        Path outputPath = tempDir.resolve("json-output.json");

        // Act
        OutputHandler.write(data, List.of("H"), Collections.emptyList(), null, true, outputPath.toString());

        // Assert
        assertThat(outputPath).exists();
        try {
            String fileContent = Files.readString(outputPath);
            assertThat(fileContent).contains("\"result\" : \"success\"");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
