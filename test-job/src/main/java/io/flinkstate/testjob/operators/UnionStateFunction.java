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

public class UnionStateFunction extends ProcessFunction<TestRecord, String>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private transient ListState<String> unionState;
    private transient long recordCount;
    private transient long lastTimestamp;
    private transient int subtaskIndex;

    @Override
    public void open(OpenContext openContext) {
        subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        unionState = context.getOperatorStateStore().getUnionListState(
            new ListStateDescriptor<>("union-records", Types.STRING));
        recordCount = 0;
        lastTimestamp = 0;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        unionState.clear();
        String summary = String.format("subtask-%d: %d records, last at %d, chk %d",
            subtaskIndex, recordCount, lastTimestamp, context.getCheckpointId());
        unionState.add(summary);
    }

    @Override
    public void processElement(TestRecord record, Context ctx, Collector<String> out)
            throws Exception {
        recordCount++;
        lastTimestamp = record.timestamp;
        out.collect("union-" + subtaskIndex + ": " + record.key);
    }
}
