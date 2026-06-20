package io.flinkstate.testjob.operators;

import io.flinkstate.testjob.TestRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class KeyedStateExerciser extends KeyedProcessFunction<String, TestRecord, String> {

    private static final long serialVersionUID = 1L;

    private final double emaAlpha;
    private final int recentValuesMaxSize;

    private transient ValueState<Double> runningAverage;
    private transient ValueState<Long> eventCount;
    private transient ValueState<String> lastCategory;
    private transient ListState<String> recentValues;
    private transient MapState<String, Long> categoryCounts;

    public KeyedStateExerciser(double emaAlpha, int recentValuesMaxSize) {
        this.emaAlpha = emaAlpha;
        this.recentValuesMaxSize = recentValuesMaxSize;
    }

    @Override
    public void open(OpenContext openContext) {
        runningAverage = getRuntimeContext().getState(
            new ValueStateDescriptor<>("running-average", Types.DOUBLE));
        eventCount = getRuntimeContext().getState(
            new ValueStateDescriptor<>("event-count", Types.LONG));
        lastCategory = getRuntimeContext().getState(
            new ValueStateDescriptor<>("last-category", Types.STRING));
        recentValues = getRuntimeContext().getListState(
            new ListStateDescriptor<>("recent-values", Types.STRING));
        categoryCounts = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("category-counts", Types.STRING, Types.LONG));
    }

    @Override
    public void processElement(TestRecord record, Context ctx, Collector<String> out)
            throws Exception {

        Double avg = runningAverage.value();
        if (avg == null) {
            avg = record.value;
        } else {
            avg = emaAlpha * record.value + (1.0 - emaAlpha) * avg;
        }
        runningAverage.update(avg);

        Long count = eventCount.value();
        eventCount.update(count == null ? 1L : count + 1L);

        lastCategory.update(record.category);

        recentValues.add(String.format("%.2f@%d", record.value, record.timestamp));
        List<String> recent = new ArrayList<>();
        for (String v : recentValues.get()) {
            recent.add(v);
        }
        if (recent.size() > recentValuesMaxSize) {
            recentValues.clear();
            List<String> trimmed = recent.subList(
                recent.size() - recentValuesMaxSize, recent.size());
            for (String v : trimmed) {
                recentValues.add(v);
            }
        }

        Long catCount = categoryCounts.get(record.category);
        categoryCounts.put(record.category, catCount == null ? 1L : catCount + 1L);

        out.collect(record.key + ": avg=" + String.format("%.2f", avg)
            + ", count=" + eventCount.value());
    }
}
