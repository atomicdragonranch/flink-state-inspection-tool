package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.discovery.DiscoveredOperator;
import io.flinkstate.inspector.discovery.MetadataReader;
import io.flinkstate.inspector.storage.StorageConnector;
import io.flinkstate.inspector.storage.StorageConnectorFactory;
import io.flinkstate.inspector.util.OutputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@CommandLine.Command(
    name = "browse",
    description = "Discover checkpoints and list their operators at a storage path."
)
public class BrowseCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BrowseCommand.class);
    private static final int DEFAULT_CHECKPOINT_LIMIT = 20;

    @CommandLine.Parameters(index = "0", arity = "0..1",
        description = "Checkpoint/savepoint path (or storage base path to discover from)")
    private String path;

    @CommandLine.Option(names = {"--limit", "-n"}, description = "Max checkpoints to discover",
        defaultValue = "20")
    private int limit;

    @CommandLine.Option(names = {"--json"}, description = "Output as raw JSON")
    private boolean json;

    @CommandLine.Option(names = {"--output", "-O"}, description = "Export results to file")
    private String outputFile;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        if (path == null || path.isEmpty()) {
            System.err.println("Error: a storage path is required.");
            return;
        }

        try (StorageConnector connector = StorageConnectorFactory.create(path, s3Options.toConfigMap())) {
            List<CheckpointEntry> checkpoints = connector.discoverCheckpoints(path, limit);

            if (checkpoints.isEmpty()) {
                System.out.println("No checkpoints or savepoints found at: " + path);
                return;
            }

            List<String> headers = List.of("#", "TYPE", "JOB", "TIME", "PATH", "OPERATORS");
            List<List<String>> rows = new ArrayList<>();
            List<Map<String, Object>> jsonResults = new ArrayList<>();

            for (int i = 0; i < checkpoints.size(); i++) {
                CheckpointEntry entry = checkpoints.get(i);
                String operatorSummary = discoverOperatorSummary(connector, entry);

                rows.add(List.of(
                    String.valueOf(i + 1),
                    String.valueOf(entry.getType()),
                    Objects.toString(entry.shortJobId(), ""),
                    Objects.toString(entry.formattedTime(), ""),
                    Objects.toString(entry.getPath(), ""),
                    operatorSummary
                ));

                if (json) {
                    Map<String, Object> cpMap = new LinkedHashMap<>();
                    cpMap.put("index", i + 1);
                    cpMap.put("type", String.valueOf(entry.getType()));
                    cpMap.put("jobId", entry.getJobId());
                    cpMap.put("shortJobId", entry.shortJobId());
                    cpMap.put("modificationTime", entry.getModificationTime());
                    cpMap.put("formattedTime", entry.formattedTime());
                    cpMap.put("path", entry.getPath());
                    cpMap.put("operators", discoverOperatorDetails(connector, entry));
                    jsonResults.add(cpMap);
                }
            }

            String footer = checkpoints.size() + " checkpoints found";

            Object jsonData = null;
            if (json) {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("basePath", path);
                result.put("checkpointCount", checkpoints.size());
                result.put("checkpoints", jsonResults);
                jsonData = result;
            }

            OutputHandler.write(jsonData, headers, rows, footer, json, outputFile);
        } catch (Exception e) {
            LOG.error("Failed to browse checkpoints at path: {}", path, e);
            System.err.println("Error: " + e.getMessage());
        }
    }

    private String discoverOperatorSummary(StorageConnector connector, CheckpointEntry entry) {
        try {
            String localPath = connector.resolveFullCheckpoint(entry.getPath());
            List<DiscoveredOperator> operators = MetadataReader.readOperatorsFromPath(localPath);
            if (operators.isEmpty()) {
                return "(none)";
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < operators.size(); i++) {
                if (i > 0) sb.append(", ");
                DiscoveredOperator op = operators.get(i);
                sb.append(op.getUid());
                List<String> stateTypes = new ArrayList<>();
                if (!op.getKeyedStates().isEmpty()) stateTypes.add("keyed");
                if (!op.getOperatorStates().isEmpty()) {
                    stateTypes.add("operator[" + op.getOperatorStates().size() + "]");
                }
                if (!stateTypes.isEmpty()) {
                    sb.append('(').append(String.join(",", stateTypes)).append(')');
                }
            }
            return sb.toString();
        } catch (Exception e) {
            LOG.debug("Could not discover operators for {}: {}", entry.getPath(), e.getMessage());
            return "(error)";
        }
    }

    private List<Map<String, Object>> discoverOperatorDetails(StorageConnector connector,
                                                               CheckpointEntry entry) {
        try {
            String localPath = connector.resolveFullCheckpoint(entry.getPath());
            List<DiscoveredOperator> operators = MetadataReader.readOperatorsFromPath(localPath);
            List<Map<String, Object>> opList = new ArrayList<>();
            for (DiscoveredOperator op : operators) {
                Map<String, Object> opMap = new LinkedHashMap<>();
                opMap.put("uid", op.getUid());
                opMap.put("name", op.getName());
                opMap.put("keyedStates", op.getKeyedStates());
                opMap.put("operatorStates", op.getOperatorStates());
                opList.add(opMap);
            }
            return opList;
        } catch (Exception e) {
            LOG.debug("Could not discover operators for {}: {}", entry.getPath(), e.getMessage());
            return List.of();
        }
    }
}
