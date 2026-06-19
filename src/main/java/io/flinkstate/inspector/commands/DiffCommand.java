package io.flinkstate.inspector.commands;

import picocli.CommandLine;

@CommandLine.Command(
    name = "diff",
    description = "Compare state between two savepoints or checkpoints."
)
public class DiffCommand implements Runnable {

    @CommandLine.Parameters(index = "0", description = "First checkpoint/savepoint path")
    private String path1;

    @CommandLine.Parameters(index = "1", description = "Second checkpoint/savepoint path")
    private String path2;

    @CommandLine.Option(names = {"--operator", "-o"}, description = "Filter by operator UID")
    private String operatorUid;

    @CommandLine.Option(names = {"--state", "-s"}, description = "Filter by state name")
    private String stateName;

    @CommandLine.Option(names = {"--keys-only", "-k"}, description = "Compare keys only, ignore values")
    private boolean keysOnly;

    @CommandLine.Option(names = {"--json"}, description = "Output as raw JSON")
    private boolean json;

    @CommandLine.Option(names = {"--output", "-O"}, description = "Export diff to file")
    private String outputFile;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        // TODO: implement with DiffExecutor + GenericKeyedStateReader
        System.out.println("Diff command not yet implemented.");
        System.out.println("  Path 1: " + path1);
        System.out.println("  Path 2: " + path2);
    }
}
