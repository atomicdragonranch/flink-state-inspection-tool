package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.util.OutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@CommandLine.Command(
    name = "diff",
    description = "Compare state between two savepoints or checkpoints."
)
public class DiffCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DiffCommand.class);

    @CommandLine.Parameters(index = "0", description = "First checkpoint/savepoint path")
    private String path1;

    @CommandLine.Parameters(index = "1", description = "Second checkpoint/savepoint path")
    private String path2;

    @CommandLine.Option(names = {"--operator", "-o"}, description = "Filter by operator UID")
    private String operatorUid;

    @CommandLine.Option(names = {"--state", "-s"}, description = "Filter by state name")
    private String stateName;

    @CommandLine.Option(names = {"--keys-only", "-k"}, description = "Compare keys only, ignore values")
    private boolean keysOnly;

    @CommandLine.Option(names = {"--json"}, description = "Output as raw JSON")
    private boolean json;

    @CommandLine.Option(names = {"--output", "-O"}, description = "Export diff to file")
    private String outputFile;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        // TODO: implement with DiffExecutor + GenericKeyedStateReader
        // When implemented, the results will be formatted through OutputHandler.
        // For now, output a placeholder message through the formatting pipeline.

        List<String> headers = List.of("STATUS", "OPERATOR", "STATE", "KEY", "LEFT VALUE", "RIGHT VALUE");
        List<List<String>> rows = Collections.emptyList();
        String footer = "Diff command not yet implemented." + System.lineSeparator()
                + "  Path 1: " + path1 + System.lineSeparator()
                + "  Path 2: " + path2;

        Object jsonData = null;
        if (json) {
            Map<String, Object> placeholder = new LinkedHashMap<>();
            placeholder.put("status", "not_implemented");
            placeholder.put("path1", path1);
            placeholder.put("path2", path2);
            placeholder.put("diffs", Collections.emptyList());
            jsonData = placeholder;
        }

        OutputHandler.write(jsonData, headers, rows, footer, json, outputFile);
    }
}
