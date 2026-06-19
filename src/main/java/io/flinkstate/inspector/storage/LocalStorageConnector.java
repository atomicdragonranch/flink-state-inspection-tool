package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.discovery.SnapshotType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalStorageConnector extends StorageConnector {

    @Override
    public String scheme() {
        return "file";
    }

    @Override
    public void initialize(Map<String, String> config) {
    }

    @Override
    public List<CheckpointEntry> discoverCheckpoints(String basePath, int limit) {
        File base = new File(basePath);
        if (!base.isDirectory()) {
            return Collections.emptyList();
        }

        List<CheckpointEntry> entries = new ArrayList<>();

        File[] jobDirs = base.listFiles(File::isDirectory);
        if (jobDirs == null) {
            return entries;
        }

        for (File jobDir : jobDirs) {
            File[] snapshotDirs = jobDir.listFiles(f ->
                f.isDirectory() && (f.getName().startsWith("chk-") || f.getName().startsWith("savepoint-")));
            if (snapshotDirs == null) {
                continue;
            }

            for (File snapshotDir : snapshotDirs) {
                SnapshotType type = snapshotDir.getName().startsWith("savepoint-")
                    ? SnapshotType.SAVEPOINT
                    : SnapshotType.CHECKPOINT;
                entries.add(new CheckpointEntry(
                    snapshotDir.getAbsolutePath(), snapshotDir.lastModified(), type, jobDir.getName()));
            }
        }

        entries.sort(Comparator.comparingLong(CheckpointEntry::getModificationTime).reversed());
        return entries.stream().limit(limit).collect(Collectors.toList());
    }

    @Override
    public boolean validateCheckpoint(String checkpointPath) {
        return new File(checkpointPath, "_metadata").exists();
    }

    @Override
    public InputStream readMetadataFile(String checkpointPath) throws IOException {
        return new FileInputStream(new File(checkpointPath, "_metadata"));
    }

    @Override
    public String resolveMetadataPath(String checkpointPath) {
        return checkpointPath;
    }

    @Override
    public String resolveFullCheckpoint(String checkpointPath) {
        return checkpointPath;
    }

    @Override
    public String resolveForFlink(String path) {
        return path;
    }

    @Override
    public List<String> listDirectories(String path) {
        File dir = new File(path);
        File[] subdirs = dir.listFiles(File::isDirectory);
        if (subdirs == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(subdirs)
            .map(File::getAbsolutePath)
            .collect(Collectors.toList());
    }
}
