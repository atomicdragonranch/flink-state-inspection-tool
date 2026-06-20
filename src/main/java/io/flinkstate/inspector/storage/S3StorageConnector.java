package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.discovery.SnapshotType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class S3StorageConnector extends StorageConnector {

    private static final Logger LOG = LoggerFactory.getLogger(S3StorageConnector.class);
    private static final String DEFAULT_REGION = "us-east-1";
    private static final int MAX_S3_OBJECTS = 10_000;

    private S3Client s3Client;
    private final AtomicReference<Path> tempDirRef = new AtomicReference<>();

    @Override
    public String scheme() {
        return "s3";
    }

    @Override
    public void initialize(Map<String, String> config) {
        LOG.info("Initializing S3 storage connector");

        String region = config.getOrDefault("aws.region",
            System.getenv().getOrDefault("AWS_DEFAULT_REGION", DEFAULT_REGION));

        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(region))
            .httpClient(UrlConnectionHttpClient.create());

        String endpoint = config.get("aws.endpoint");
        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }

        if ("true".equals(config.get("aws.path-style-access"))) {
            builder.forcePathStyle(true);
        }

        String accessKey = config.get("aws.access-key-id");
        String secretKey = config.get("aws.secret-access-key");
        if (accessKey != null && secretKey != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        this.s3Client = builder.build();
        LOG.info("S3 client initialized for region {}", region);
    }

    @Override
    public List<CheckpointEntry> discoverCheckpoints(String basePath, int limit) {
        String[] parsed = parseS3Uri(basePath);
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
                String path = "s3://" + bucket + "/" + snapshotPrefix;
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
        String[] parsed = parseS3Uri(checkpointPath);
        String metadataKey = parsed[1].isEmpty() ? "_metadata" : parsed[1] + "/_metadata";
        try {
            s3Client.headObject(HeadObjectRequest.builder()
                .bucket(parsed[0])
                .key(metadataKey)
                .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public InputStream readMetadataFile(String checkpointPath) throws IOException {
        String[] parsed = parseS3Uri(checkpointPath);
        String metadataKey = parsed[1].isEmpty() ? "_metadata" : parsed[1] + "/_metadata";
        return s3Client.getObject(GetObjectRequest.builder()
            .bucket(parsed[0])
            .key(metadataKey)
            .build());
    }

    @Override
    public String resolveMetadataPath(String checkpointPath) throws IOException {
        String[] parsed = parseS3Uri(checkpointPath);
        String bucket = parsed[0];
        String metadataKey = parsed[1].isEmpty() ? "_metadata" : parsed[1] + "/_metadata";

        Path localDir = createLocalDir("meta-");
        Path localFile = localDir.resolve("_metadata");

        s3Client.getObject(
            GetObjectRequest.builder().bucket(bucket).key(metadataKey).build(),
            localFile);
        LOG.info("Downloaded _metadata from s3://{}/{} to {}", bucket, metadataKey, localFile);
        return localDir.toString();
    }

    @Override
    public String resolveFullCheckpoint(String checkpointPath) throws IOException {
        String[] parsed = parseS3Uri(checkpointPath);
        String bucket = parsed[0];
        String prefix = parsed[1].isEmpty() ? "" : parsed[1] + "/";

        Path localDir = createLocalDir("full-chk-");

        List<S3Object> objects = listAllObjects(bucket, prefix);
        if (objects.size() > MAX_S3_OBJECTS) {
            throw new IOException("Checkpoint contains " + objects.size()
                + " objects, exceeding the safety limit of " + MAX_S3_OBJECTS
                + ". Use a more specific path or increase the limit.");
        }
        int count = 0;
        for (S3Object obj : objects) {
            String relativePath = obj.key().substring(prefix.length());
            if (relativePath.isEmpty()) continue;

            Path localFile = localDir.resolve(relativePath.replace('/', java.io.File.separatorChar));
            Files.createDirectories(localFile.getParent());
            s3Client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(obj.key()).build(),
                localFile);
            count++;
        }
        LOG.info("Downloaded {}/{} files from s3://{}/{} to {}", count, objects.size(), bucket, prefix, localDir);
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
        String[] parsed = parseS3Uri(path);
        String prefix = parsed[1].isEmpty() ? "" : parsed[1] + "/";
        return listPrefixes(parsed[0], prefix).stream()
            .map(p -> "s3://" + parsed[0] + "/" + (p.endsWith("/") ? p.substring(0, p.length() - 1) : p))
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
            Path newDir = Files.createTempDirectory("flink-s3-inspector-");
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

    private List<S3Object> listAllObjects(String bucket, String prefix) {
        List<S3Object> all = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix);
            if (continuationToken != null) {
                reqBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = s3Client.listObjectsV2(reqBuilder.build());
            all.addAll(response.contents());
            continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
        } while (continuationToken != null);
        return all;
    }

    private List<String> listPrefixes(String bucket, String prefix) {
        ListObjectsV2Response response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(bucket)
            .prefix(prefix)
            .delimiter("/")
            .build());
        return response.commonPrefixes().stream()
            .map(CommonPrefix::prefix)
            .collect(Collectors.toList());
    }

    private long getMetadataLastModified(String bucket, String snapshotPrefix) {
        String metadataKey = snapshotPrefix + "_metadata";
        try {
            return s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(metadataKey)
                .build())
                .lastModified()
                .toEpochMilli();
        } catch (Exception e) {
            LOG.debug("Could not retrieve modification time for s3://{}/{}: {}", bucket, metadataKey, e.getMessage());
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
            LOG.debug("Cleaning up temp directory: {}", tempDir);
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
        if (s3Client != null) {
            try {
                s3Client.close();
            } catch (Exception e) {
                LOG.warn("Failed to close S3 client: {}", e.getMessage());
            }
            LOG.info("S3 client closed");
        }
    }

    AtomicReference<Path> getTempDirRef() {
        return tempDirRef;
    }

    S3Client getS3Client() {
        return s3Client;
    }

    static String[] parseS3Uri(String uri) {
        String stripped = uri.replaceFirst("^s3a?://", "");
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
