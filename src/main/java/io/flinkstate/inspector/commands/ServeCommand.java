package io.flinkstate.inspector.commands;

import picocli.CommandLine;

@CommandLine.Command(
    name = "serve",
    description = "Start a REST API for state inspection."
)
public class ServeCommand implements Runnable {

    @CommandLine.Option(names = {"--port", "-p"}, description = "HTTP port", defaultValue = "9741")
    private int port;

    @Override
    public void run() {
        // TODO: implement Javalin REST API
        System.out.println("Starting REST API on port " + port + "...");
        System.out.println("Serve command not yet implemented.");
    }
}
