package io.flinkstate.inspector.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.discovery.SnapshotType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Storage connector for Google Cloud Storage.
 *
 * Uses the Google Cloud Storage client library to list, download, and validate
 * Flink checkpoint/savepoint data stored in GCS buckets.
 *
 * Supported config keys:
 *   gcs.credentials  - Path to service account key file (overrides GOOGLE_APPLICATION_CREDENTIALS)
 *   gcs.project      - GCP project ID (optional)
 */
public class GcsStorageConnector extends StorageConnector {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorageConnector.class);
    private static final int MAX_GCS_OBJECTS = 10_000;

    private Storage storageClient;
    private final AtomicReference<Path> tempDirRef = new AtomicReference<>();

    @Override
    public String scheme() {
        return "gs";
    }

    @Override
    public void initialize(Map<String, String> config) {
        LOG.info("Initializing GCS storage connector");

        StorageOptions.Builder builder = StorageOptions.newBuilder();

        String project = config.get("gcs.project");
        if (project != null && !project.isEmpty()) {
            builder.setProjectId(project);
        }

        String credentialsPath = config.get("gcs.credentials");
        if (credentialsPath != null && !credentialsPath.isEmpty()) {
            try (FileInputStream fis = new FileInputStream(credentialsPath)) {
                GoogleCredentials credentials = ServiceAccountCredentials.fromStream(fis);
                builder.setCredentials(credentials);
                LOG.info("Using service account credentials from {}", credentialsPath);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load GCS credentials from " + credentialsPath, e);
            }
        }

        this.storageClient = builder.build().getService();
        LOG.info("GCS client initialized" + (project != null ? " for project " + project : ""));
    }

    @Override
    public List<CheckpointEntry> discoverCheckpoints(String basePath, int limit) {
        String[] parsed = parseGcsUri(basePath);
        String bucket = parsed[0];
        String prefix = parsed[1].isEmpty() ? "" : parsed[1] + "/";

        List<CheckpointEntry> entries = new ArrayList<>();

        List<String> jobPrefixes = listPrefixes(bucket, prefix);
        for (String jobPrefix : jobPrefixes) {
            String jobId = extractName(jobPrefix);
            List<String> snapshotPrefixes = listPrefixes(bucket, jobPrefix);
            for (String snapshotPrefix : snapshotPrefixes) {
                String snapshotName = extractName(snapshotPrefix);
                if (!snapshotName.startsWith("chk-") && !snapshotName.startsWith("savepoint-")) {
                    continue;
                }

                SnapshotType type = snapshotName.startsWith("savepoint-")
                    ? SnapshotType.SAVEPOINT
                    : SnapshotType.CHECKPOINT;

                long modTime = getMetadataLastModified(bucket, snapshotPrefix);
                String path = "gs://" + bucket + "/" + snapshotPrefix;
                if (path.endsWith("/")) {
                    path = path.substring(0, path.length() - 1);
                }
                entries.add(new CheckpointEntry(path, modTime, type, jobId));
            }
        }

        entries.sort(Comparator.comparingLong(CheckpointEntry::getModificationTime).reversed());
        return entries.stream().limit(limit).collect(Collectors.toList());
    }

    @Override
    public boolean validateCheckpoint(String checkpointPath) {
        String[] parsed = parseGcsUri(checkpointPath);
        String metadataKey = parsed[1].isEmpty() ? "_metadata" : parsed[1] + "/_metadata";
        Blob blob = storageClient.get(BlobId.of(parsed[0], metadataKey));
        return blob != null && blob.exists();
    }

    @Override
    public InputStream readMetadataFile(String checkpointPath) throws IOException {
        String[] parsed = parseGcsUri(checkpointPath);
        String metadataKey = parsed[1].isEmpty() ? "_metadata" : parsed[1] + "/_metadata";
        Blob blob = storageClient.get(BlobId.of(parsed[0], metadataKey));
        if (blob == null || !blob.exists()) {
            throw new IOException("Metadata file not found: gs://" + parsed[0] + "/" + metadataKey);
        }
        return new ByteArrayInputStream(blob.getContent());
    }

    @Override
    public String resolveMetadataPath(String checkpointPath) throws IOException {
        String[] parsed = parseGcsUri(checkpointPath);
        String bucket = parsed[0];
        String metadataKey = parsed[1].isEmpty() ? "_metadata" : parsed[1] + "/_metadata";

        Path localDir = createLocalDir("meta-");
        Path localFile = localDir.resolve("_metadata");

        Blob blob = storageClient.get(BlobId.of(bucket, metadataKey));
        if (blob == null || !blob.exists()) {
            throw new IOException("Metadata file not found: gs://" + bucket + "/" + metadataKey);
        }
        blob.downloadTo(localFile);
        LOG.info("Downloaded _metadata from gs://{}/{} to {}", bucket, metadataKey, localFile);
        return localDir.toString();
    }

    @Override
    public String resolveFullCheckpoint(String checkpointPath) throws IOException {
        String[] parsed = parseGcsUri(checkpointPath);
        String bucket = parsed[0];
        String prefix = parsed[1].isEmpty() ? "" : parsed[1] + "/";

        Path localDir = createLocalDir("full-chk-");

        List<Blob> blobs = listAllBlobs(bucket, prefix);
        if (blobs.size() > MAX_GCS_OBJECTS) {
            throw new IOException("Checkpoint contains " + blobs.size()
                + " objects, exceeding the safety limit of " + MAX_GCS_OBJECTS
                + ". Use a more specific path or increase the limit.");
        }
        int count = 0;
        for (Blob blob : blobs) {
            String relativePath = blob.getName().substring(prefix.length());
            if (relativePath.isEmpty()) continue;

            Path localFile = localDir.resolve(relativePath.replace('/', java.io.File.separatorChar));
            Files.createDirectories(localFile.getParent());
            blob.downloadTo(localFile);
            count++;
        }
        LOG.info("Downloaded {}/{} files from gs://{}/{} to {}", count, blobs.size(), bucket, prefix, localDir);
        return localDir.toString();
    }

    @Override
    public String resolveForFlink(String path) {
        try {
            return resolveFullCheckpoint(path);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Override
    public List<String> listDirectories(String path) {
        String[] parsed = parseGcsUri(path);
        String prefix = parsed[1].isEmpty() ? "" : parsed[1] + "/";
        return listPrefixes(parsed[0], prefix).stream()
            .map(p -> "gs://" + parsed[0] + "/" + (p.endsWith("/") ? p.substring(0, p.length() - 1) : p))
            .collect(Collectors.toList());
    }

    private Path createLocalDir(String prefix) throws IOException {
        ensureTempDir();
        Path localDir = tempDirRef.get().resolve(prefix + UUID.randomUUID().toString().substring(0, 8));
        Files.createDirectories(localDir);
        return localDir;
    }

    void ensureTempDir() throws IOException {
        if (tempDirRef.get() == null) {
            Path newDir = Files.createTempDirectory("flink-gcs-inspector-");
            try {
                Files.setPosixFilePermissions(newDir,
                    PosixFilePermissions.fromString("rwx------"));
            } catch (UnsupportedOperationException e) {
                LOG.debug("POSIX permissions not supported on this OS; skipping");
            }
            if (!tempDirRef.compareAndSet(null, newDir)) {
                Files.deleteIfExists(newDir);
            }
        }
    }

    private List<Blob> listAllBlobs(String bucket, String prefix) {
        List<Blob> all = new ArrayList<>();
        for (Blob blob : storageClient.list(bucket,
                Storage.BlobListOption.prefix(prefix)).iterateAll()) {
            all.add(blob);
        }
        return all;
    }

    private List<String> listPrefixes(String bucket, String prefix) {
        Set<String> prefixes = new HashSet<>();
        for (Blob blob : storageClient.list(bucket,
                Storage.BlobListOption.prefix(prefix),
                Storage.BlobListOption.currentDirectory()).iterateAll()) {
            if (blob.isDirectory()) {
                prefixes.add(blob.getName());
            }
        }
        return new ArrayList<>(prefixes);
    }

    private long getMetadataLastModified(String bucket, String snapshotPrefix) {
        String metadataKey = snapshotPrefix + "_metadata";
        try {
            Blob blob = storageClient.get(BlobId.of(bucket, metadataKey));
            if (blob != null && blob.exists() && blob.getUpdateTimeOffsetDateTime() != null) {
                return blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();
            }
            return 0L;
        } catch (Exception e) {
            return 0L;
        }
    }

    private static String extractName(String prefix) {
        String trimmed = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        int slash = trimmed.lastIndexOf('/');
        return slash >= 0 ? trimmed.substring(slash + 1) : trimmed;
    }

    @Override
    public void close() {
        Path tempDir = tempDirRef.get();
        if (tempDir != null) {
            LOG.info("Cleaning up temp directory: {}", tempDir);
            try {
                Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
            } catch (IOException e) {
                LOG.warn("Failed to clean up temp directory: {}", tempDir, e);
            }
        }
        if (storageClient != null) {
            try {
                storageClient.close();
                LOG.info("GCS client closed");
            } catch (Exception e) {
                LOG.warn("Failed to close GCS client", e);
            }
        }
    }

    AtomicReference<Path> getTempDirRef() {
        return tempDirRef;
    }

    Storage getStorageClient() {
        return storageClient;
    }

    static String[] parseGcsUri(String uri) {
        String stripped = uri.replaceFirst("^gs://", "");
        int slash = stripped.indexOf('/');
        if (slash < 0) {
            return new String[]{stripped, ""};
        }
        String key = stripped.substring(slash + 1);
        if (key.endsWith("/")) {
            key = key.substring(0, key.length() - 1);
        }
        return new String[]{stripped.substring(0, slash), key};
    }
}
