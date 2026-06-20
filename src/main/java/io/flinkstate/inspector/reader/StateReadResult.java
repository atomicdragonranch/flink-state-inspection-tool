package io.flinkstate.inspector.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class StateReadResult {

    private final String operatorUid;
    private final List<Map<String, Object>> entries;
    private final List<String> columns;
    private final int skippedSstFiles;
    private final List<String> warnings;

    public StateReadResult(String operatorUid, List<Map<String, Object>> entries,
                           List<String> columns) {
        this(operatorUid, entries, columns, 0, List.of());
    }

    public StateReadResult(String operatorUid, List<Map<String, Object>> entries,
                           List<String> columns, int skippedSstFiles,
                           List<String> warnings) {
        this.operatorUid = operatorUid;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.columns = columns;
        this.skippedSstFiles = skippedSstFiles;
        this.warnings = warnings != null
            ? Collections.unmodifiableList(new ArrayList<>(warnings))
            : List.of();
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

    public int getSkippedSstFiles() {
        return skippedSstFiles;
    }

    public List<String> getWarnings() {
        return warnings;
    }
}
