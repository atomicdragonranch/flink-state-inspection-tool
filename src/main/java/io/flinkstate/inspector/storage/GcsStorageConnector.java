package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Storage connector for Google Cloud Storage.
 *
 * Configures Flink's GCS filesystem plugin (flink-gs-fs-hadoop) and handles
 * authentication via GOOGLE_APPLICATION_CREDENTIALS or Application Default Credentials.
 *
 * Required plugin: flink-gs-fs-hadoop JAR in the plugins/ directory.
 *
 * Supported config keys:
 *   gcs.credentials  - Path to service account key file (overrides GOOGLE_APPLICATION_CREDENTIALS)
 *   gcs.project      - GCP project ID
 */
public class GcsStorageConnector extends StorageConnector {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorageConnector.class);

    @Override
    public String scheme() {
        return "gs";
    }

    @Override
    public void initialize(Map<String, String> config) {
        LOG.info("Initializing GCS storage connector");
        // TODO: configure Flink's GCS filesystem with Google credentials
        // org.apache.flink.core.fs.FileSystem.initialize(flinkConfig, null);
    }

    @Override
    public List<CheckpointEntry> discoverCheckpoints(String basePath, int limit) {
        throw new UnsupportedOperationException("GCS connector not yet implemented");
    }

    @Override
    public boolean validateCheckpoint(String checkpointPath) {
        throw new UnsupportedOperationException("GCS connector not yet implemented");
    }

    @Override
    public InputStream readMetadataFile(String checkpointPath) throws IOException {
        throw new UnsupportedOperationException("GCS connector not yet implemented");
    }

    @Override
    public String resolveMetadataPath(String checkpointPath) throws IOException {
        throw new UnsupportedOperationException("GCS connector not yet implemented");
    }

    @Override
    public String resolveFullCheckpoint(String checkpointPath) throws IOException {
        throw new UnsupportedOperationException("GCS connector not yet implemented");
    }

    @Override
    public String resolveForFlink(String path) {
        return path;
    }

    @Override
    public List<String> listDirectories(String path) {
        throw new UnsupportedOperationException("GCS connector not yet implemented");
    }
}
