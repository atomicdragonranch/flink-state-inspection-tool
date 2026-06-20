package io.flinkstate.testjob.generators;

import io.flinkstate.testjob.TestRecord;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class TestRecordGeneratorFunction implements GeneratorFunction<Long, TestRecord> {

    private static final long serialVersionUID = 1L;

    private final int keyCount;
    private final String[] categories;

    public TestRecordGeneratorFunction(int keyCount, String[] categories) {
        this.keyCount = keyCount;
        this.categories = categories;
    }

    @Override
    public TestRecord map(Long sequence) {
        String key = "sensor-" + (sequence % keyCount);
        String category = categories[(int) (sequence / keyCount % categories.length)];
        double value = 20.0 + (sequence % 1000) * 0.1 + Math.sin(sequence * 0.01) * 10.0;
        long timestamp = System.currentTimeMillis();
        return new TestRecord(key, category, value, timestamp);
    }
}
