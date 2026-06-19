package io.flinkstate.inspector.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public final class ErrorDisplay {

    private ErrorDisplay() {
    }

    public static String extractRootCause(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getClass().getSimpleName() + ": " + current.getMessage();
    }

    public static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
