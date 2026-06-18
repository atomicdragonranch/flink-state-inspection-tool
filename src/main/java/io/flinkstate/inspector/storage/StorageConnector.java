package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;

import java.util.List;
import java.util.Map;

/**
 * Abstract base for storage backends that host Flink checkpoint/savepoint data.
 *
 * Implementations handle authentication, directory listing, and path resolution
 * for a specific storage system. Extend this class to add support for a new
 * storage backend (e.g., Azure Blob Storage, HDFS, MinIO).
 */
public abstract class StorageConnector implements AutoCloseable {

    /**
     * URI scheme this connector handles (e.g., "file", "s3", "gs", "docker").
     */
    public abstract String scheme();

    /**
     * Set up authentication and any storage-specific configuration.
     * Called once before any read operations.
     *
     * @param config key-value pairs from CLI flags or environment variables
     */
    public abstract void initialize(Map<String, String> config);

    /**
     * Scan a base path for checkpoint and savepoint directories.
     * Expects the standard Flink layout: basePath/jobId/chk-N/ or basePath/savepoint-xyz/.
     *
     * @param basePath root directory to scan
     * @param limit    max entries to return
     * @return discovered checkpoints, newest first
     */
    public abstract List<CheckpointEntry> discoverCheckpoints(String basePath, int limit);

    /**
     * Check whether a path contains a valid Flink checkpoint (has a _metadata file).
     */
    public abstract boolean validateCheckpoint(String checkpointPath);

    /**
     * Resolve a storage path to one that Flink's SavepointReader can open directly.
     *
     * Most connectors return the path unchanged. The Docker connector copies
     * checkpoint data to a local temp directory first, since Flink cannot read
     * from inside a container.
     *
     * @param path original storage path
     * @return path that Flink's FileSystem API can open
     */
    public abstract String resolveForFlink(String path);

    /**
     * List immediate subdirectories at the given path.
     */
    public abstract List<String> listDirectories(String path);

    @Override
    public void close() {
    }
}
