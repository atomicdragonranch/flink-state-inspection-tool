package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.discovery.CheckpointEntry;
import io.flinkstate.inspector.storage.StorageConnector;
import io.flinkstate.inspector.storage.StorageConnectorFactory;
import picocli.CommandLine;

import java.util.List;

@CommandLine.Command(
    name = "list",
    description = "List checkpoints and savepoints at a storage path."
)
public class ListCommand implements Runnable {

    @CommandLine.Parameters(index = "0", description = "Storage path (local, s3://, gs://, docker://container/path)")
    private String path;

    @CommandLine.Option(names = {"--limit", "-n"}, description = "Max entries to show", defaultValue = "20")
    private int limit;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        try (StorageConnector connector = StorageConnectorFactory.create(path, s3Options.toConfigMap())) {
            List<CheckpointEntry> entries = connector.discoverCheckpoints(path, limit);

            if (entries.isEmpty()) {
                System.out.println("No checkpoints or savepoints found at: " + path);
                return;
            }

            System.out.printf("%-12s %-10s %-25s %s%n", "TYPE", "JOB", "TIME", "PATH");
            System.out.printf("%-12s %-10s %-25s %s%n", "----", "---", "----", "----");

            for (CheckpointEntry entry : entries) {
                System.out.printf("%-12s %-10s %-25s %s%n",
                    entry.getType(),
                    entry.shortJobId(),
                    entry.formattedTime(),
                    entry.getPath());
            }

            System.out.printf("%n%d entries found%n", entries.size());
        }
    }
}
