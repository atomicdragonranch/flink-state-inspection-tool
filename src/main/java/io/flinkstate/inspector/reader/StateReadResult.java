package io.flinkstate.inspector.reader;

import java.util.List;
import java.util.Map;

public final class StateReadResult {

    private final String operatorUid;
    private final List<Map<String, Object>> entries;
    private final List<String> columns;

    public StateReadResult(String operatorUid, List<Map<String, Object>> entries,
                           List<String> columns) {
        this.operatorUid = operatorUid;
        this.entries = entries;
        this.columns = columns;
    }

    public String getOperatorUid() {
        return operatorUid;
    }

    public List<Map<String, Object>> getEntries() {
        return entries;
    }

    public List<String> getColumns() {
        return columns;
    }

    public int getEntryCount() {
        return entries.size();
    }
}
