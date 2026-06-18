package io.flinkstate.inspector.discovery;

import java.time.Instant;

public class CheckpointEntry {

    private final String path;
    private final long modificationTime;
    private final SnapshotType type;
    private final String jobId;

    public CheckpointEntry(String path, long modificationTime, SnapshotType type, String jobId) {
        this.path = path;
        this.modificationTime = modificationTime;
        this.type = type;
        this.jobId = jobId;
    }

    public String getPath() {
        return path;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public SnapshotType getType() {
        return type;
    }

    public String getJobId() {
        return jobId;
    }

    public String shortJobId() {
        if (jobId == null) return null;
        return jobId.length() > 8 ? jobId.substring(0, 8) : jobId;
    }

    public String displayName() {
        String name = path.contains("/")
            ? path.substring(path.lastIndexOf('/') + 1)
            : path.substring(path.lastIndexOf('\\') + 1);
        return shortJobId() != null ? shortJobId() + "/" + name : name;
    }

    public String formattedTime() {
        return Instant.ofEpochMilli(modificationTime).toString();
    }
}
