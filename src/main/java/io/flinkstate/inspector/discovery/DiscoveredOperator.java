package io.flinkstate.inspector.discovery;

import java.util.List;

public class DiscoveredOperator {

    private final String uid;
    private final String name;
    private final List<String> keyedStates;
    private final List<String> operatorStates;
    private int keyedStateEntryCount = -1;

    public DiscoveredOperator(String uid, String name,
                              List<String> keyedStates, List<String> operatorStates) {
        this.uid = uid;
        this.name = name;
        this.keyedStates = keyedStates;
        this.operatorStates = operatorStates;
    }

    public String getUid() {
        return uid;
    }

    public String getName() {
        return name;
    }

    public List<String> getKeyedStates() {
        return keyedStates;
    }

    public List<String> getOperatorStates() {
        return operatorStates;
    }

    public int getKeyedStateEntryCount() {
        return keyedStateEntryCount;
    }

    public void setKeyedStateEntryCount(int keyedStateEntryCount) {
        this.keyedStateEntryCount = keyedStateEntryCount;
    }
}
