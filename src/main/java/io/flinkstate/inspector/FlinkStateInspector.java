package io.flinkstate.inspector;

import io.flinkstate.inspector.commands.BrowseCommand;
import io.flinkstate.inspector.commands.DiffCommand;
import io.flinkstate.inspector.commands.InspectCommand;
import io.flinkstate.inspector.commands.ListCommand;
import io.flinkstate.inspector.commands.ServeCommand;
import picocli.CommandLine;

@CommandLine.Command(
    name = "flink-state-inspector",
    description = "Auto-discovery CLI for inspecting Apache Flink savepoint and checkpoint state.",
    mixinStandardHelpOptions = true,
    version = "1.0.0-SNAPSHOT",
    subcommands = {
        ListCommand.class,
        InspectCommand.class,
        DiffCommand.class,
        BrowseCommand.class,
        ServeCommand.class
    }
)
public class FlinkStateInspector implements Runnable {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new FlinkStateInspector()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}
