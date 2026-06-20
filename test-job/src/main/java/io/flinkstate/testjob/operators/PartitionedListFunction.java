package io.flinkstate.testjob.operators;

import io.flinkstate.testjob.TestRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class PartitionedListFunction extends ProcessFunction<TestRecord, String>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final int bufferMaxSize;
    private transient List<String> buffer;
    private transient ListState<String> partitionedState;

    public PartitionedListFunction(int bufferMaxSize) {
        this.bufferMaxSize = bufferMaxSize;
    }

    @Override
    public void open(OpenContext openContext) {
        if (buffer == null) {
            buffer = new ArrayList<>();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        partitionedState = context.getOperatorStateStore().getListState(
            new ListStateDescriptor<>("partitioned-records", Types.STRING));

        buffer = new ArrayList<>();
        for (String s : partitionedState.get()) {
            buffer.add(s);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        partitionedState.clear();
        for (String s : buffer) {
            partitionedState.add(s);
        }
    }

    @Override
    public void processElement(TestRecord record, Context ctx, Collector<String> out)
            throws Exception {
        String entry = record.key + ":" + record.category + "="
            + String.format("%.2f", record.value);

        if (buffer.size() >= bufferMaxSize) {
            buffer.remove(0);
        }
        buffer.add(entry);

        out.collect(entry);
    }
}
