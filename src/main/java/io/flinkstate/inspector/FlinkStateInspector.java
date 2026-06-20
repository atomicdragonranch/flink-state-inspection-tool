package io.flinkstate.inspector;

import io.flinkstate.inspector.commands.BrowseCommand;
import io.flinkstate.inspector.commands.DiffCommand;
import io.flinkstate.inspector.commands.InspectCommand;
import io.flinkstate.inspector.commands.ListCommand;
import io.flinkstate.inspector.commands.ServeCommand;
import picocli.CommandLine;

import java.io.ObjectInputFilter;

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

    /**
     * JEP 290 deserialization filter pattern.
     * Allows only Flink and core Java classes during checkpoint metadata deserialization,
     * rejecting everything else to prevent deserialization gadget attacks.
     */
    static final String DESERIALIZATION_FILTER_PATTERN = "org.apache.flink.**;java.**;!*";

    public static void main(String[] args) {
        installDeserializationFilter();
        int exitCode = new CommandLine(new FlinkStateInspector()).execute(args);
        System.exit(exitCode);
    }

    /**
     * Installs a process-wide JEP 290 deserialization filter that restricts
     * which classes can be deserialized via ObjectInputStream. This must be
     * called before any checkpoint metadata is loaded.
     */
    static void installDeserializationFilter() {
        ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(DESERIALIZATION_FILTER_PATTERN);
        ObjectInputFilter.Config.setSerialFilter(filter);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}
