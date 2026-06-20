package io.flinkstate.testjob;

public class TestRecord {

    public String key;
    public String category;
    public double value;
    public long timestamp;

    public TestRecord() {
    }

    public TestRecord(String key, String category, double value, long timestamp) {
        this.key = key;
        this.category = category;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return key + "/" + category + "=" + String.format("%.2f", value);
    }
}
