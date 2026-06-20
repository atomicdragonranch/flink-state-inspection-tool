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
import java.util.UUID;
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

    public String register(String sourcePath, String localPath) {
        String id = UUID.randomUUID().toString().substring(0, 8);
        long size = computeDirectorySize(new File(localPath));
        entries.put(id, new CacheEntry(id, sourcePath, localPath, System.currentTimeMillis(), size));
        LOG.info("Cached checkpoint [{}]: {} -> {} ({} bytes)", id, sourcePath, localPath, size);
        return id;
    }

    public String lookupLocalPath(String sourcePath) {
        for (CacheEntry entry : entries.values()) {
            if (entry.sourcePath().equals(sourcePath)) {
                File dir = new File(entry.localPath());
                if (dir.exists()) return entry.localPath();
            }
        }
        return null;
    }

    public List<Map<String, Object>> listEntries() {
        return entries.values().stream()
            .sorted(Comparator.comparingLong(CacheEntry::cachedAt).reversed())
            .map(CacheEntry::toPublicMap)
            .collect(Collectors.toList());
    }

    public boolean delete(String id) {
        CacheEntry entry = entries.remove(id);
        if (entry == null) return false;

        File dir = new File(entry.localPath());
        if (dir.exists()) {
            try {
                Files.walk(dir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
                LOG.info("Deleted cached checkpoint [{}]: {}", id, entry.sourcePath());
            } catch (IOException e) {
                LOG.warn("Failed to delete cached checkpoint [{}]: {}", id, entry.localPath(), e);
            }
        }
        return true;
    }

    public int size() {
        return entries.size();
    }

    public void clear() {
        for (String id : List.copyOf(entries.keySet())) {
            delete(id);
        }
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

    record CacheEntry(String id, String sourcePath, String localPath, long cachedAt, long sizeBytes) {
        Map<String, Object> toPublicMap() {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put("id", id);
            map.put("sourcePath", sourcePath);
            map.put("cachedAt", cachedAt);
            map.put("sizeBytes", sizeBytes);
            return map;
        }
    }
}
