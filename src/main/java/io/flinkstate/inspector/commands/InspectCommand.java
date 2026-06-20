package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.util.OutputHandler;
import picocli.CommandLine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@CommandLine.Command(
    name = "inspect",
    description = "Auto-discover and inspect state in a savepoint or checkpoint."
)
public class InspectCommand implements Runnable {

    @CommandLine.Parameters(index = "0", description = "Checkpoint or savepoint path")
    private String path;

    @CommandLine.Option(names = {"--operator", "-o"}, description = "Filter by operator UID")
    private String operatorUid;

    @CommandLine.Option(names = {"--state", "-s"}, description = "Filter by state name")
    private String stateName;

    @CommandLine.Option(names = {"--keys-only", "-k"}, description = "Show only state keys, not values")
    private boolean keysOnly;

    @CommandLine.Option(names = {"--key-filter"}, description = "Filter entries by key pattern (substring match)")
    private String keyFilter;

    @CommandLine.Option(names = {"--json"}, description = "Output as raw JSON")
    private boolean json;

    @CommandLine.Option(names = {"--pretty"}, description = "Pretty-print JSON output", defaultValue = "true")
    private boolean pretty;

    @CommandLine.Option(names = {"--output", "-O"}, description = "Export results to file")
    private String outputFile;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        // TODO: implement with OperatorDiscovery + GenericKeyedStateReader
        // When implemented, the results will be formatted through OutputHandler.
        // For now, output a placeholder message through the formatting pipeline.

        List<String> headers = List.of("OPERATOR", "STATE", "KEY", "VALUE");
        List<List<String>> rows = Collections.emptyList();
        String footer = "Inspect command not yet implemented. Path: " + path;

        Object jsonData = null;
        if (json) {
            Map<String, Object> placeholder = new LinkedHashMap<>();
            placeholder.put("status", "not_implemented");
            placeholder.put("path", path);
            placeholder.put("entries", Collections.emptyList());
            jsonData = placeholder;
        }

        OutputHandler.write(jsonData, headers, rows, footer, json, outputFile);
    }
}
