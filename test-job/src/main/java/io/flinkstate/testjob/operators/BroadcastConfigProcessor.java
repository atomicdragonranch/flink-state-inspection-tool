package io.flinkstate.testjob.operators;

import io.flinkstate.testjob.TestRecord;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastConfigProcessor
        extends KeyedBroadcastProcessFunction<String, TestRecord, String, String> {

    private static final long serialVersionUID = 1L;

    private final MapStateDescriptor<String, String> configDescriptor;

    public BroadcastConfigProcessor(MapStateDescriptor<String, String> configDescriptor) {
        this.configDescriptor = configDescriptor;
    }

    @Override
    public void processElement(TestRecord record, ReadOnlyContext ctx, Collector<String> out)
            throws Exception {
        ReadOnlyBroadcastState<String, String> config = ctx.getBroadcastState(configDescriptor);

        String threshold = config.get("threshold");
        String mode = config.get("mode");
        String version = config.get("version");

        out.collect(String.format("%s: value=%.2f, threshold=%s, mode=%s, version=%s",
            record.key, record.value,
            threshold != null ? threshold : "unset",
            mode != null ? mode : "unset",
            version != null ? version : "unset"));
    }

    @Override
    public void processBroadcastElement(String configEntry, Context ctx, Collector<String> out)
            throws Exception {
        String[] parts = configEntry.split("=", 2);
        if (parts.length == 2) {
            ctx.getBroadcastState(configDescriptor).put(parts[0], parts[1]);
        }
    }
}
