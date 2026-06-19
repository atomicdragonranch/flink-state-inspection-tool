package io.flinkstate.inspector.commands;

import picocli.CommandLine;

@CommandLine.Command(
    name = "browse",
    description = "Interactively browse checkpoint state with auto-discovery."
)
public class BrowseCommand implements Runnable {

    @CommandLine.Parameters(index = "0", arity = "0..1",
        description = "Checkpoint/savepoint path (or storage base path to discover from)")
    private String path;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        // TODO: implement interactive browser with auto-discovered operators
        System.out.println("Browse command not yet implemented.");
    }
}
