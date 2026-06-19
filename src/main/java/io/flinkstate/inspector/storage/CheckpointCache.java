package io.flinkstate.inspector.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class CheckpointCache {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCache.class);
    private static final CheckpointCache INSTANCE = new CheckpointCache();

    private final ConcurrentHashMap<String, CacheEntry> entries = new ConcurrentHashMap<>();

    private CheckpointCache() {
    }

    public static CheckpointCache getInstance() {
        return INSTANCE;
    }

    public void register(String sourcePath, String localPath) {
        long size = computeDirectorySize(new File(localPath));
        entries.put(localPath, new CacheEntry(sourcePath, localPath, System.currentTimeMillis(), size));
        LOG.info("Cached checkpoint: {} -> {} ({} bytes)", sourcePath, localPath, size);
    }

    public List<Map<String, Object>> listEntries() {
        return entries.values().stream()
            .sorted(Comparator.comparingLong(CacheEntry::cachedAt).reversed())
            .map(CacheEntry::toMap)
            .collect(Collectors.toList());
    }

    public boolean delete(String localPath) {
        CacheEntry entry = entries.remove(localPath);
        if (entry == null) return false;

        File dir = new File(localPath);
        if (dir.exists()) {
            try {
                Files.walk(dir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
                LOG.info("Deleted cached checkpoint: {}", localPath);
            } catch (IOException e) {
                LOG.warn("Failed to delete cached checkpoint: {}", localPath, e);
            }
        }
        return true;
    }

    public int size() {
        return entries.size();
    }

    private static long computeDirectorySize(File dir) {
        if (!dir.isDirectory()) return 0;
        long total = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                total += f.isFile() ? f.length() : computeDirectorySize(f);
            }
        }
        return total;
    }

    record CacheEntry(String sourcePath, String localPath, long cachedAt, long sizeBytes) {
        Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("sourcePath", sourcePath);
            map.put("localPath", localPath);
            map.put("cachedAt", cachedAt);
            map.put("sizeBytes", sizeBytes);
            return map;
        }
    }
}
