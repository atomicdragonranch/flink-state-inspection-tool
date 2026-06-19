package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Storage connector for AWS S3.
 *
 * Configures Flink's S3 filesystem plugin (flink-s3-fs-hadoop) and handles
 * AWS authentication via environment variables or instance profile.
 *
 * Required plugin: flink-s3-fs-hadoop JAR in the plugins/ directory.
 *
 * Supported config keys:
 *   aws.region       - AWS region (default: from AWS_DEFAULT_REGION env var)
 *   aws.endpoint     - Custom S3 endpoint (for MinIO or other S3-compatible stores)
 */
public class S3StorageConnector extends StorageConnector {

    private static final Logger LOG = LoggerFactory.getLogger(S3StorageConnector.class);

    @Override
    public String scheme() {
        return "s3";
    }

    @Override
    public void initialize(Map<String, String> config) {
        LOG.info("Initializing S3 storage connector");
        // TODO: configure Flink's S3 filesystem with AWS credentials
        // org.apache.flink.core.fs.FileSystem.initialize(flinkConfig, null);
    }

    @Override
    public List<CheckpointEntry> discoverCheckpoints(String basePath, int limit) {
        throw new UnsupportedOperationException("S3 connector not yet implemented");
    }

    @Override
    public boolean validateCheckpoint(String checkpointPath) {
        throw new UnsupportedOperationException("S3 connector not yet implemented");
    }

    @Override
    public InputStream readMetadataFile(String checkpointPath) throws IOException {
        throw new UnsupportedOperationException("S3 connector not yet implemented");
    }

    @Override
    public String resolveMetadataPath(String checkpointPath) throws IOException {
        throw new UnsupportedOperationException("S3 connector not yet implemented");
    }

    @Override
    public String resolveFullCheckpoint(String checkpointPath) throws IOException {
        throw new UnsupportedOperationException("S3 connector not yet implemented");
    }

    @Override
    public String resolveForFlink(String path) {
        return path;
    }

    @Override
    public List<String> listDirectories(String path) {
        throw new UnsupportedOperationException("S3 connector not yet implemented");
    }
}
