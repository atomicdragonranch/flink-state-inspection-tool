package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.storage.StorageConnector;
import io.flinkstate.inspector.storage.StorageConnectorFactory;
import io.flinkstate.inspector.util.OutputHandler;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@CommandLine.Command(
    name = "list",
    description = "List checkpoints and savepoints at a storage path."
)
public class ListCommand implements Runnable {

    @CommandLine.Parameters(index = "0", description = "Storage path (local, s3://, gs://, docker://container/path)")
    private String path;

    @CommandLine.Option(names = {"--limit", "-n"}, description = "Max entries to show", defaultValue = "20")
    private int limit;

    @CommandLine.Option(names = {"--json"}, description = "Output as raw JSON")
    private boolean json;

    @CommandLine.Option(names = {"--output", "-O"}, description = "Export results to file")
    private String outputFile;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        try (StorageConnector connector = StorageConnectorFactory.create(path, s3Options.toConfigMap())) {
            List<CheckpointEntry> entries = connector.discoverCheckpoints(path, limit);

            if (entries.isEmpty()) {
                System.out.println("No checkpoints or savepoints found at: " + path);
                return;
            }

            List<String> headers = List.of("TYPE", "JOB", "TIME", "PATH");
            List<List<String>> rows = new ArrayList<>();

            for (CheckpointEntry entry : entries) {
                rows.add(List.of(
                    String.valueOf(entry.getType()),
                    nullSafe(entry.shortJobId()),
                    nullSafe(entry.formattedTime()),
                    nullSafe(entry.getPath())
                ));
            }

            String footer = entries.size() + " entries found";

            Object jsonData = null;
            if (json) {
                jsonData = toJsonStructure(entries);
            }

            OutputHandler.write(jsonData, headers, rows, footer, json, outputFile);
        }
    }

    private List<Map<String, Object>> toJsonStructure(List<CheckpointEntry> entries) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (CheckpointEntry entry : entries) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("type", String.valueOf(entry.getType()));
            map.put("jobId", entry.getJobId());
            map.put("shortJobId", entry.shortJobId());
            map.put("modificationTime", entry.getModificationTime());
            map.put("formattedTime", entry.formattedTime());
            map.put("path", entry.getPath());
            result.add(map);
        }
        return result;
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }
}
