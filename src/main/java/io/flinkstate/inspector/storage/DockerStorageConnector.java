package io.flinkstate.inspector.storage;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.discovery.SnapshotType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Storage connector that reads checkpoint data from inside Docker containers.
 *
 * Uses the docker CLI (docker exec, docker cp) to access files inside running
 * containers. This is useful for local development where Flink writes checkpoints
 * to the container filesystem rather than an external volume.
 *
 * URI format: docker://container-name/path/inside/container
 *
 * Example: docker://flink-taskmanager/opt/flink/checkpoints
 */
public class DockerStorageConnector extends StorageConnector {

    private static final Logger LOG = LoggerFactory.getLogger(DockerStorageConnector.class);

    private Path tempDir;

    @Override
    public String scheme() {
        return "docker";
    }

    @Override
    public void initialize(Map<String, String> config) {
        LOG.info("Initializing Docker storage connector");
    }

    @Override
    public List<CheckpointEntry> discoverCheckpoints(String basePath, int limit) {
        String[] parsed = parseDockerUri(basePath);
        String container = parsed[0];
        String containerPath = parsed[1];

        List<String> metadataFiles = execInContainer(
            container, "find", containerPath, "-name", "_metadata", "-maxdepth", "3");

        List<CheckpointEntry> entries = new ArrayList<>();
        for (String metadataPath : metadataFiles) {
            String chkDir = metadataPath.replace("/_metadata", "");
            String chkName = chkDir.substring(chkDir.lastIndexOf('/') + 1);
            String parentDir = chkDir.substring(0, chkDir.lastIndexOf('/'));
            String jobId = parentDir.substring(parentDir.lastIndexOf('/') + 1);

            SnapshotType type = chkName.startsWith("savepoint-")
                ? SnapshotType.SAVEPOINT
                : SnapshotType.CHECKPOINT;

            entries.add(new CheckpointEntry(
                "docker://" + container + chkDir, System.currentTimeMillis(), type, jobId));

            if (entries.size() >= limit) {
                break;
            }
        }
        return entries;
    }

    @Override
    public boolean validateCheckpoint(String checkpointPath) {
        String[] parsed = parseDockerUri(checkpointPath);
        List<String> result = execInContainer(
            parsed[0], "sh", "-c", "test -f '" + parsed[1] + "/_metadata' && echo ok");
        return result.stream().anyMatch(line -> line.contains("ok"));
    }

    @Override
    public InputStream readMetadataFile(String checkpointPath) throws IOException {
        String localDir = resolveMetadataPath(checkpointPath);
        return new FileInputStream(new File(localDir, "_metadata"));
    }

    @Override
    public String resolveMetadataPath(String checkpointPath) throws IOException {
        String[] parsed = parseDockerUri(checkpointPath);
        String container = parsed[0];
        String containerMetadataPath = parsed[1] + "/_metadata";

        if (tempDir == null) {
            tempDir = Files.createTempDirectory("flink-state-inspector-");
        }
        Path localDir = tempDir.resolve("chk-" + UUID.randomUUID().toString().substring(0, 8));
        Files.createDirectories(localDir);
        Path localFile = localDir.resolve("_metadata");
        dockerCp(container, containerMetadataPath, localFile.toString());
        LOG.info("Copied _metadata from {}:{} to {}", container, containerMetadataPath, localDir);
        return localDir.toString();
    }

    @Override
    public String resolveFullCheckpoint(String checkpointPath) throws IOException {
        String[] parsed = parseDockerUri(checkpointPath);
        String container = parsed[0];
        String containerPath = parsed[1];

        if (tempDir == null) {
            tempDir = Files.createTempDirectory("flink-state-inspector-");
        }
        Path localDir = tempDir.resolve("full-chk-" + UUID.randomUUID().toString().substring(0, 8));
        Files.createDirectories(localDir);
        dockerCp(container, containerPath + "/.", localDir.toString());
        LOG.info("Copied full checkpoint from {}:{} to {}", container, containerPath, localDir);
        return localDir.toString();
    }

    @Override
    public String resolveForFlink(String path) {
        String[] parsed = parseDockerUri(path);
        try {
            if (tempDir == null) {
                tempDir = Files.createTempDirectory("flink-state-inspector-");
            }
            Path localDir = tempDir.resolve(UUID.randomUUID().toString());
            Files.createDirectories(localDir);

            dockerCp(parsed[0], parsed[1], localDir.toString());
            LOG.info("Copied checkpoint from container {} to {}", parsed[0], localDir);
            return localDir.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to copy checkpoint from container", e);
        }
    }

    @Override
    public List<String> listDirectories(String path) {
        String[] parsed = parseDockerUri(path);
        return execInContainer(parsed[0], "ls", "-1", parsed[1]);
    }

    @Override
    public void close() {
        if (tempDir != null) {
            LOG.info("Cleaning up temp directory: {}", tempDir);
            try {
                Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException e) {
                LOG.warn("Failed to clean up temp directory: {}", tempDir, e);
            }
        }
    }

    static String[] parseDockerUri(String uri) {
        String stripped = uri.replaceFirst("^docker://", "");
        int slash = stripped.indexOf('/');
        if (slash < 0) {
            return new String[]{stripped, "/"};
        }
        return new String[]{stripped.substring(0, slash), stripped.substring(slash)};
    }

    private List<String> execInContainer(String container, String... command) {
        try {
            List<String> fullCommand = new ArrayList<>();
            fullCommand.add("docker");
            fullCommand.add("exec");
            fullCommand.add(container);
            fullCommand.addAll(Arrays.asList(command));

            Process process = new ProcessBuilder(fullCommand)
                .redirectErrorStream(true)
                .start();

            List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        lines.add(line.trim());
                    }
                }
            }
            process.waitFor();
            return lines;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Docker exec failed: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private void dockerCp(String container, String containerPath, String localPath) {
        try {
            Process process = new ProcessBuilder("docker", "cp",
                container + ":" + containerPath, localPath)
                .start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("docker cp exited with code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Docker cp interrupted", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
