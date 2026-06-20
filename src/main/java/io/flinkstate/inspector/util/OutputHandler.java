package io.flinkstate.inspector.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Handles output routing for CLI commands.
 * Supports JSON vs table formatting and stdout vs file output.
 */
public final class OutputHandler {

    private static final Logger LOG = LoggerFactory.getLogger(OutputHandler.class);

    private static final ObjectMapper PRETTY_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private OutputHandler() {
    }

    /**
     * Write output in the requested format to the requested destination.
     *
     * @param data       the data to serialize (used for JSON mode)
     * @param headers    table column headers (used for table mode)
     * @param rows       table row data (used for table mode)
     * @param footer     optional footer line (e.g., "5 entries found"); null to skip
     * @param json       true to output JSON, false for table format
     * @param outputFile path to write output to a file; null for stdout
     */
    public static void write(Object data, List<String> headers, List<List<String>> rows,
                              String footer, boolean json, String outputFile) {
        String content;
        if (json) {
            content = toJson(data);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(TableFormatter.formatTable(headers, rows));
            if (footer != null && !footer.isEmpty()) {
                sb.append(System.lineSeparator());
                sb.append(footer);
                sb.append(System.lineSeparator());
            }
            content = sb.toString();
        }

        if (outputFile != null && !outputFile.isEmpty()) {
            try {
                writeToFile(content, outputFile);
            } catch (IOException e) {
                LOG.error("Failed to write output to file: {}", outputFile, e);
                System.err.println("Failed to write output to file: " + e.getMessage());
            }
        } else {
            System.out.print(content);
        }
    }

    /**
     * Serialize an object to pretty-printed JSON.
     *
     * @param data the object to serialize
     * @return JSON string
     */
    public static String toJson(Object data) {
        try {
            return PRETTY_MAPPER.writeValueAsString(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }

    /**
     * Write content to a file, creating parent directories if needed.
     *
     * @param content    the string content to write
     * @param filePath   the output file path
     * @throws IOException if file creation or writing fails
     */
    public static void writeToFile(String content, String filePath) throws IOException {
        Path path = Path.of(filePath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.writeString(path, content, StandardCharsets.UTF_8);
        System.err.println("Output written to: " + filePath);
    }
}
