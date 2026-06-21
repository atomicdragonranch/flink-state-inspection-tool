package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.api.DiffEndpoint;
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
    name = "diff",
    description = "Compare state between two savepoints or checkpoints."
)
public class DiffCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DiffCommand.class);
    private static final int DEFAULT_LIMIT = 10_000;

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

    @CommandLine.Option(names = {"--limit", "-n"}, description = "Max entries per diff category", defaultValue = "10000")
    private int limit;

    @CommandLine.Option(names = {"--json"}, description = "Output as raw JSON")
    private boolean json;

    @CommandLine.Option(names = {"--output", "-O"}, description = "Export diff to file")
    private String outputFile;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        try {
            Map<String, String> config = s3Options.toConfigMap();
            String localPath1;
            String localPath2;

            try (StorageConnector connector1 = StorageConnectorFactory.create(path1, config)) {
                localPath1 = connector1.resolveFullCheckpoint(path1);
            }
            try (StorageConnector connector2 = StorageConnectorFactory.create(path2, config)) {
                localPath2 = connector2.resolveFullCheckpoint(path2);
            }

            LOG.info("Diff: resolved path1={}, path2={}", localPath1, localPath2);

            List<DiscoveredOperator> operators1 = MetadataReader.readOperatorsFromPath(localPath1);

            if (operatorUid != null) {
                operators1 = operators1.stream()
                    .filter(op -> op.getUid().equals(operatorUid))
                    .collect(java.util.stream.Collectors.toList());
                if (operators1.isEmpty()) {
                    System.err.println("No operator found with UID: " + operatorUid);
                    return;
                }
            }

            List<String> headers = List.of("STATUS", "OPERATOR", "KEY", "LEFT VALUE", "RIGHT VALUE");
            List<List<String>> allRows = new ArrayList<>();
            List<Map<String, Object>> allDiffResults = new ArrayList<>();

            for (DiscoveredOperator op : operators1) {
                if (!op.getKeyedStates().isEmpty()) {
                    diffKeyedState(localPath1, localPath2, op, allRows, allDiffResults);
                }

                for (String opStateName : op.getOperatorStates()) {
                    if (stateName != null && !stateName.equals(opStateName)) {
                        continue;
                    }
                    diffOperatorState(localPath1, localPath2, op, opStateName, allRows, allDiffResults);
                }
            }

            String footer = allRows.size() + " diff entries found";

            Object jsonData = null;
            if (json) {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("path1", path1);
                result.put("path2", path2);
                result.put("diffs", allDiffResults);
                jsonData = result;
            }

            OutputHandler.write(jsonData, headers, allRows, footer, json, outputFile);
        } catch (Exception e) {
            LOG.error("Failed to diff checkpoints: path1={}, path2={}", path1, path2, e);
            System.err.println("Error: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void diffKeyedState(String localPath1, String localPath2,
                                DiscoveredOperator op,
                                List<List<String>> rows,
                                List<Map<String, Object>> diffResults) {
        try {
            StateReadResult result1 = GenericStateReader.readKeyedState(
                localPath1, op.getUid(), null, keysOnly, limit);
            StateReadResult result2 = GenericStateReader.readKeyedState(
                localPath2, op.getUid(), null, keysOnly, limit);

            Map<String, Object> diff = DiffEndpoint.computeDiff(
                result1, result2, op.getUid(), path1, path2);

            appendDiffRows(diff, op.getUid(), rows);

            diff.put("stateType", "keyed");
            diffResults.add(diff);
        } catch (Exception e) {
            LOG.warn("Failed to diff keyed state for operator {}: {}", op.getUid(), e.getMessage());
            System.err.println("Warning: could not diff keyed state for " + op.getUid()
                + " - " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void diffOperatorState(String localPath1, String localPath2,
                                   DiscoveredOperator op, String opStateName,
                                   List<List<String>> rows,
                                   List<Map<String, Object>> diffResults) {
        try {
            StateReadResult result1 = OperatorStateReader.readOperatorState(
                localPath1, op.getUid(), opStateName, null, limit);
            StateReadResult result2 = OperatorStateReader.readOperatorState(
                localPath2, op.getUid(), opStateName, null, limit);

            Map<String, Object> diff = DiffEndpoint.computeDiff(
                result1, result2, op.getUid(), path1, path2);

            appendDiffRows(diff, op.getUid(), rows);

            diff.put("stateType", "operator");
            diff.put("stateName", opStateName);
            diffResults.add(diff);
        } catch (Exception e) {
            LOG.warn("Failed to diff operator state {} for operator {}: {}",
                opStateName, op.getUid(), e.getMessage());
            System.err.println("Warning: could not diff operator state '" + opStateName
                + "' for " + op.getUid() + " - " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void appendDiffRows(Map<String, Object> diff, String opUid,
                                List<List<String>> rows) {
        List<Map<String, Object>> added = (List<Map<String, Object>>) diff.get("added");
        List<Map<String, Object>> removed = (List<Map<String, Object>>) diff.get("removed");
        List<Map<String, Object>> modified = (List<Map<String, Object>>) diff.get("modified");

        for (Map<String, Object> entry : added) {
            String key = String.valueOf(entry.get("key"));
            String rightVal = Objects.toString(entry.get("json2"), "");
            rows.add(List.of("ADDED", opUid, key, "", rightVal));
        }
        for (Map<String, Object> entry : removed) {
            String key = String.valueOf(entry.get("key"));
            String leftVal = Objects.toString(entry.get("json1"), "");
            rows.add(List.of("REMOVED", opUid, key, leftVal, ""));
        }
        for (Map<String, Object> entry : modified) {
            String key = String.valueOf(entry.get("key"));
            String leftVal = Objects.toString(entry.get("json1"), "");
            String rightVal = Objects.toString(entry.get("json2"), "");
            rows.add(List.of("MODIFIED", opUid, key, leftVal, rightVal));
        }
    }
}
