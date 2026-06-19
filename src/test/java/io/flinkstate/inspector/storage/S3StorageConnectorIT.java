package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class S3StorageConnectorIT {

    private static final DockerImageName LOCALSTACK_IMAGE =
        DockerImageName.parse("localstack/localstack:3.5.0");

    @Container
    static LocalStackContainer localStack = new LocalStackContainer(LOCALSTACK_IMAGE)
        .withServices(LocalStackContainer.Service.S3);

    private S3StorageConnector connector;
    private S3Client s3Client;
    private String bucket;

    @BeforeEach
    void setUp() {
        bucket = "test-" + UUID.randomUUID().toString().substring(0, 8);

        s3Client = S3Client.builder()
            .endpointOverride(localStack.getEndpoint())
            .region(Region.of(localStack.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
            .httpClient(UrlConnectionHttpClient.create())
            .forcePathStyle(true)
            .build();

        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

        connector = new S3StorageConnector();
        connector.initialize(Map.of(
            "aws.region", localStack.getRegion(),
            "aws.endpoint", localStack.getEndpoint().toString(),
            "aws.path-style-access", "true",
            "aws.access-key-id", localStack.getAccessKey(),
            "aws.secret-access-key", localStack.getSecretKey()
        ));
    }

    @AfterEach
    void tearDown() {
        connector.close();
        s3Client.close();
    }

    @Test
    void discoverCheckpointsFindsCheckpointDirs() {
        // Arrange
        uploadFakeCheckpoint("job-abc123/chk-1");
        uploadFakeCheckpoint("job-abc123/chk-2");

        // Act
        List<CheckpointEntry> entries = connector.discoverCheckpoints("s3://" + bucket, 50);

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getJobId()).isEqualTo("job-abc123");
        assertThat(entries.get(0).getPath()).contains("chk-");
    }

    @Test
    void discoverCheckpointsDistinguishesSavepoints() {
        // Arrange
        uploadFakeCheckpoint("job-xyz/chk-1");
        uploadFakeCheckpoint("job-xyz/savepoint-abc");

        // Act
        List<CheckpointEntry> entries = connector.discoverCheckpoints("s3://" + bucket, 50);

        // Assert
        assertThat(entries).hasSize(2);
        assertThat(entries).anyMatch(e -> e.getType().name().equals("CHECKPOINT"));
        assertThat(entries).anyMatch(e -> e.getType().name().equals("SAVEPOINT"));
    }

    @Test
    void discoverCheckpointsReturnsEmptyForEmptyBucket() {
        // Arrange (bucket exists but is empty)

        // Act
        List<CheckpointEntry> entries = connector.discoverCheckpoints("s3://" + bucket, 50);

        // Assert
        assertThat(entries).isEmpty();
    }

    @Test
    void discoverCheckpointsRespectsLimit() {
        // Arrange
        uploadFakeCheckpoint("job-1/chk-1");
        uploadFakeCheckpoint("job-1/chk-2");
        uploadFakeCheckpoint("job-1/chk-3");

        // Act
        List<CheckpointEntry> entries = connector.discoverCheckpoints("s3://" + bucket, 2);

        // Assert
        assertThat(entries).hasSize(2);
    }

    @Test
    void validateCheckpointReturnsTrueWhenMetadataExists() {
        // Arrange
        uploadFakeCheckpoint("job-1/chk-1");

        // Act
        boolean valid = connector.validateCheckpoint("s3://" + bucket + "/job-1/chk-1");

        // Assert
        assertThat(valid).isTrue();
    }

    @Test
    void validateCheckpointReturnsFalseWhenMissing() {
        // Arrange (nothing uploaded)

        // Act
        boolean valid = connector.validateCheckpoint("s3://" + bucket + "/nonexistent/chk-99");

        // Assert
        assertThat(valid).isFalse();
    }

    @Test
    void readMetadataFileReturnsContents() throws IOException {
        // Arrange
        uploadFakeCheckpoint("job-1/chk-1");

        // Act
        InputStream stream = connector.readMetadataFile("s3://" + bucket + "/job-1/chk-1");
        byte[] content = stream.readAllBytes();
        stream.close();

        // Assert
        assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo("fake-metadata");
    }

    @Test
    void resolveMetadataPathDownloadsToTempDir() throws IOException {
        // Arrange
        uploadFakeCheckpoint("job-1/chk-1");

        // Act
        String localPath = connector.resolveMetadataPath("s3://" + bucket + "/job-1/chk-1");

        // Assert
        File metadataFile = new File(localPath, "_metadata");
        assertThat(metadataFile).exists();
        assertThat(metadataFile).hasContent("fake-metadata");
    }

    @Test
    void resolveFullCheckpointDownloadsAllFiles() throws IOException {
        // Arrange
        uploadFakeCheckpoint("job-1/chk-1");

        // Act
        String localPath = connector.resolveFullCheckpoint("s3://" + bucket + "/job-1/chk-1");

        // Assert
        assertThat(new File(localPath, "_metadata")).exists();
        assertThat(new File(localPath, "000001.sst")).exists();
        assertThat(new File(localPath, "000002.sst")).exists();
    }

    @Test
    void listDirectoriesReturnsSubdirectories() {
        // Arrange
        uploadFakeCheckpoint("job-aaa/chk-1");
        uploadFakeCheckpoint("job-bbb/chk-1");

        // Act
        List<String> dirs = connector.listDirectories("s3://" + bucket);

        // Assert
        assertThat(dirs).hasSize(2);
        assertThat(dirs).anyMatch(d -> d.contains("job-aaa"));
        assertThat(dirs).anyMatch(d -> d.contains("job-bbb"));
    }

    @Test
    void closeCleansTempDirectory() throws IOException {
        // Arrange
        uploadFakeCheckpoint("job-1/chk-1");
        String localPath = connector.resolveMetadataPath("s3://" + bucket + "/job-1/chk-1");
        File localDir = new File(localPath);
        assertThat(localDir).exists();

        // Act
        connector.close();

        // Assert
        assertThat(localDir).doesNotExist();
    }

    private void uploadFakeCheckpoint(String prefix) {
        putObject(prefix + "/_metadata", "fake-metadata");
        putObject(prefix + "/000001.sst", "fake-sst-data-1");
        putObject(prefix + "/000002.sst", "fake-sst-data-2");
    }

    private void putObject(String key, String content) {
        s3Client.putObject(
            PutObjectRequest.builder().bucket(bucket).key(key).build(),
            RequestBody.fromString(content));
    }
}
