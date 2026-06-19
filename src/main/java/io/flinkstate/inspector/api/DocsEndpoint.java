package io.flinkstate.inspector.api;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class DocsEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DocsEndpoint.class);

    private DocsEndpoint() {
    }

    public static void register(Javalin app) {
        app.get("/api/docs/{name}", ctx -> {
            String name = ctx.pathParam("name");

            if (name.contains("..") || name.contains("/") || name.contains("\\")) {
                ctx.status(400);
                ctx.json(ApiResponse.error("Invalid document name: " + name));
                return;
            }

            String resourcePath = "/docs/" + name + ".md";
            InputStream is = DocsEndpoint.class.getResourceAsStream(resourcePath);

            if (is == null) {
                LOG.debug("Document not found: {}", resourcePath);
                ctx.status(404);
                ctx.json(ApiResponse.error("Document not found: " + name));
                return;
            }

            try {
                String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                ctx.json(ApiResponse.success(content));
            } finally {
                is.close();
            }
        });

        app.get("/api/docs", ctx -> {
            ctx.json(ApiResponse.success(new String[]{"state-inspector-guide"}));
        });
    }
}
