package io.flinkstate.inspector.discovery;

import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class MetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataReader.class);

    private MetadataReader() {
    }

    public static List<DiscoveredOperator> readOperatorsFromPath(String localCheckpointDir) throws Exception {
        File metadataFile = new File(localCheckpointDir, "_metadata");
        try (FileInputStream fis = new FileInputStream(metadataFile);
             DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {

            CheckpointMetadata metadata = Checkpoints.loadCheckpointMetadata(
                dis, Thread.currentThread().getContextClassLoader(), localCheckpointDir);

            LOG.info("Loaded checkpoint {} with {} operator states",
                metadata.getCheckpointId(), metadata.getOperatorStates().size());

            return extractOperators(metadata);
        }
    }

    private static List<DiscoveredOperator> extractOperators(CheckpointMetadata metadata) {
        List<DiscoveredOperator> operators = new ArrayList<>();
        for (OperatorState opState : metadata.getOperatorStates()) {
            if (opState.isFullyFinished()) {
                continue;
            }

            String uid = opState.getOperatorUid()
                .orElse(opState.getOperatorID().toHexString());
            String name = opState.getOperatorName().orElse("");

            boolean hasKeyedState = false;
            Set<String> operatorStateNames = new LinkedHashSet<>();

            for (OperatorSubtaskState subtaskState : opState.getSubtaskStates().values()) {
                if (!subtaskState.getManagedKeyedState().isEmpty()) {
                    hasKeyedState = true;
                }
                for (OperatorStateHandle handle : subtaskState.getManagedOperatorState()) {
                    operatorStateNames.addAll(
                        handle.getStateNameToPartitionOffsets().keySet());
                }
            }

            if (!hasKeyedState && operatorStateNames.isEmpty()) {
                continue;
            }

            List<String> keyedStates = hasKeyedState
                ? List.of("keyed-state")
                : List.of();

            operators.add(new DiscoveredOperator(
                uid, name, keyedStates, new ArrayList<>(operatorStateNames)));

            LOG.info("Discovered operator: uid={}, name={}, keyed={}, operatorStates={}",
                uid, name, hasKeyedState, operatorStateNames);
        }
        return operators;
    }
}
