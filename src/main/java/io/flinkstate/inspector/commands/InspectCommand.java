package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.discovery.DiscoveredOperator;
import io.flinkstate.inspector.discovery.MetadataReader;
import io.flinkstate.inspector.reader.GenericStateReader;
import io.flinkstate.inspector.reader.OperatorStateReader;
import io.flinkstate.inspector.reader.StateReadResult;
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
    name = "inspect",
    description = "Auto-discover and inspect state in a savepoint or checkpoint."
)
public class InspectCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(InspectCommand.class);
    private static final int DEFAULT_LIMIT = 1000;

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

    @CommandLine.Option(names = {"--limit", "-n"}, description = "Max entries to show", defaultValue = "1000")
    private int limit;

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
        try (StorageConnector connector = StorageConnectorFactory.create(path, s3Options.toConfigMap())) {
            String localPath = connector.resolveFullCheckpoint(path);
            LOG.info("Resolved checkpoint to local path: {}", localPath);

            List<DiscoveredOperator> operators = MetadataReader.readOperatorsFromPath(localPath);

            if (operatorUid != null) {
                operators = operators.stream()
                    .filter(op -> op.getUid().equals(operatorUid))
                    .collect(java.util.stream.Collectors.toList());
                if (operators.isEmpty()) {
                    System.err.println("No operator found with UID: " + operatorUid);
                    return;
                }
            }

            List<String> headers = keysOnly
                ? List.of("OPERATOR", "KEY")
                : List.of("OPERATOR", "STATE", "KEY", "VALUE");
            List<List<String>> allRows = new ArrayList<>();
            List<Map<String, Object>> allJsonEntries = new ArrayList<>();

            for (DiscoveredOperator op : operators) {
                LOG.info("Inspecting operator: uid={}, keyed={}, operatorStates={}",
                    op.getUid(), !op.getKeyedStates().isEmpty(), op.getOperatorStates());

                if (!op.getKeyedStates().isEmpty()) {
                    inspectKeyedState(localPath, op, allRows, allJsonEntries);
                }

                for (String opStateName : op.getOperatorStates()) {
                    if (stateName != null && !stateName.equals(opStateName)) {
                        continue;
                    }
                    inspectOperatorState(localPath, op, opStateName, allRows, allJsonEntries);
                }
            }

            String footer = allRows.size() + " entries found";

            Object jsonData = null;
            if (json) {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("path", path);
                result.put("entryCount", allJsonEntries.size());
                result.put("entries", allJsonEntries);
                jsonData = result;
            }

            OutputHandler.write(jsonData, headers, allRows, footer, json, outputFile);
        } catch (Exception e) {
            LOG.error("Failed to inspect checkpoint at path: {}", path, e);
            System.err.println("Error: " + e.getMessage());
        }
    }

    private void inspectKeyedState(String localPath, DiscoveredOperator op,
                                   List<List<String>> rows,
                                   List<Map<String, Object>> jsonEntries) {
        try {
            StateReadResult result = GenericStateReader.readKeyedState(
                localPath, op.getUid(), keyFilter, keysOnly, limit);

            for (Map<String, Object> entry : result.getEntries()) {
                String key = String.valueOf(entry.get("key"));

                if (keysOnly) {
                    rows.add(List.of(op.getUid(), key));
                } else {
                    for (String col : result.getColumns()) {
                        if ("key".equals(col)) continue;
                        String value = Objects.toString(entry.get(col), "");
                        rows.add(List.of(op.getUid(), col, key, value));
                    }
                }

                Map<String, Object> jsonEntry = new LinkedHashMap<>(entry);
                jsonEntry.put("operatorUid", op.getUid());
                jsonEntry.put("stateType", "keyed");
                jsonEntries.add(jsonEntry);
            }

            for (String warning : result.getWarnings()) {
                System.err.println("Warning: " + warning);
            }
        } catch (Exception e) {
            LOG.warn("Failed to read keyed state for operator {}: {}", op.getUid(), e.getMessage());
            System.err.println("Warning: could not read keyed state for " + op.getUid()
                + " - " + e.getMessage());
        }
    }

    private void inspectOperatorState(String localPath, DiscoveredOperator op,
                                      String opStateName,
                                      List<List<String>> rows,
                                      List<Map<String, Object>> jsonEntries) {
        try {
            StateReadResult result = OperatorStateReader.readOperatorState(
                localPath, op.getUid(), opStateName, keyFilter, limit);

            for (Map<String, Object> entry : result.getEntries()) {
                String key = entry.containsKey("key")
                    ? String.valueOf(entry.get("key"))
                    : String.valueOf(entry.get("partition"));
                String value = Objects.toString(entry.get("value"), "");

                if (keysOnly) {
                    rows.add(List.of(op.getUid(), key));
                } else {
                    rows.add(List.of(op.getUid(), opStateName, key, value));
                }

                Map<String, Object> jsonEntry = new LinkedHashMap<>(entry);
                jsonEntry.put("operatorUid", op.getUid());
                jsonEntry.put("stateType", "operator");
                jsonEntry.put("stateName", opStateName);
                jsonEntries.add(jsonEntry);
            }
        } catch (Exception e) {
            LOG.warn("Failed to read operator state {} for operator {}: {}",
                opStateName, op.getUid(), e.getMessage());
            System.err.println("Warning: could not read operator state '" + opStateName
                + "' for " + op.getUid() + " - " + e.getMessage());
        }
    }
}
