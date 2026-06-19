package io.flinkstate.inspector.commands;

import io.flinkstate.inspector.api.ApiResponse;
import io.flinkstate.inspector.api.CacheEndpoint;
import io.flinkstate.inspector.api.DiffEndpoint;
import io.flinkstate.inspector.api.DiscoveryEndpoint;
import io.flinkstate.inspector.api.DocsEndpoint;
import io.flinkstate.inspector.api.InspectEndpoint;
import io.flinkstate.inspector.api.OperatorEndpoint;
import io.flinkstate.inspector.util.ErrorDisplay;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "serve",
    description = "Start the web UI and REST API for state inspection."
)
public class ServeCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServeCommand.class);

    @CommandLine.Option(names = {"--port", "-p"}, description = "HTTP port", defaultValue = "9741")
    private int port;

    @CommandLine.Option(names = {"--host"}, description = "Bind address", defaultValue = "0.0.0.0")
    private String host;

    @CommandLine.Mixin
    private S3Options s3Options;

    @Override
    public void run() {
        Javalin app = Javalin.create(config -> {
            config.staticFiles.add("/public", Location.CLASSPATH);
            config.spaRoot.addFile("/", "/public/index.html", Location.CLASSPATH);
        });

        app.exception(Exception.class, (e, ctx) -> {
            LOG.debug("API error", e);
            ctx.status(500);
            ctx.json(ApiResponse.error(
                ErrorDisplay.extractRootCause(e),
                ErrorDisplay.getStackTrace(e)));
        });

        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            LOG.debug("Bad request", e);
            ctx.status(400);
            ctx.json(ApiResponse.error(e.getMessage()));
        });

        app.get("/health", ctx -> ctx.result("ok"));

        DiscoveryEndpoint.register(app);
        OperatorEndpoint.register(app);
        InspectEndpoint.register(app);
        DiffEndpoint.register(app);
        DocsEndpoint.register(app);
        CacheEndpoint.register(app);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down...");
            app.stop();
        }));

        app.start(host, port);
        System.out.println();
        System.out.println("  Flink State Inspector");
        System.out.println("  =====================");
        System.out.println("  UI:  http://localhost:" + port);
        System.out.println("  API: http://localhost:" + port + "/api/checkpoints/discover");
        System.out.println();
        System.out.println("  Press Ctrl+C to stop.");
        System.out.println();

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
