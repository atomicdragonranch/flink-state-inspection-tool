package io.flinkstate.inspector.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Fixed-width column formatter for CLI table output.
 * Handles column alignment, truncation, and separator rows.
 */
public final class TableFormatter {

    /** Default maximum width for any single column. */
    public static final int DEFAULT_MAX_COLUMN_WIDTH = 60;

    /** Minimum column width (must fit at least a few characters plus ellipsis). */
    public static final int MIN_COLUMN_WIDTH = 5;

    /** Padding between columns. */
    public static final int COLUMN_PADDING = 2;

    /** Ellipsis suffix appended when values are truncated. */
    public static final String TRUNCATION_SUFFIX = "...";

    private TableFormatter() {
    }

    /**
     * Formats tabular data with auto-sized columns capped at maxColumnWidth.
     *
     * @param headers     column header labels
     * @param rows        row data (each inner list corresponds to one row)
     * @return formatted table string with header, separator, and data rows
     */
    public static String formatTable(List<String> headers, List<List<String>> rows) {
        return formatTable(headers, rows, DEFAULT_MAX_COLUMN_WIDTH);
    }

    /**
     * Formats tabular data with auto-sized columns capped at the given max width.
     *
     * @param headers        column header labels
     * @param rows           row data
     * @param maxColumnWidth maximum width for any single column
     * @return formatted table string
     */
    public static String formatTable(List<String> headers, List<List<String>> rows, int maxColumnWidth) {
        if (headers == null || headers.isEmpty()) {
            return "";
        }

        int columnCount = headers.size();
        int[] widths = computeColumnWidths(headers, rows, columnCount, maxColumnWidth);

        StringBuilder sb = new StringBuilder();

        // Header row
        sb.append(formatRow(headers, widths, columnCount));
        sb.append(System.lineSeparator());

        // Separator row
        sb.append(formatSeparator(widths, columnCount));
        sb.append(System.lineSeparator());

        // Data rows
        for (List<String> row : rows) {
            sb.append(formatRow(row, widths, columnCount));
            sb.append(System.lineSeparator());
        }

        return sb.toString();
    }

    /**
     * Compute column widths by scanning headers and all rows, capped at maxColumnWidth.
     */
    private static int[] computeColumnWidths(List<String> headers, List<List<String>> rows,
                                              int columnCount, int maxColumnWidth) {
        int[] widths = new int[columnCount];

        // Start with header widths
        for (int i = 0; i < columnCount; i++) {
            widths[i] = safeLength(headers.get(i));
        }

        // Expand based on data
        for (List<String> row : rows) {
            for (int i = 0; i < columnCount && i < row.size(); i++) {
                widths[i] = Math.max(widths[i], safeLength(row.get(i)));
            }
        }

        // Cap at max width
        for (int i = 0; i < columnCount; i++) {
            widths[i] = Math.min(widths[i], maxColumnWidth);
            widths[i] = Math.max(widths[i], MIN_COLUMN_WIDTH);
        }

        return widths;
    }

    /**
     * Format a single row, right-aligning numeric values.
     */
    private static String formatRow(List<String> values, int[] widths, int columnCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            String value = i < values.size() ? safeValue(values.get(i)) : "";
            String truncated = truncate(value, widths[i]);

            if (i > 0) {
                sb.append(" ".repeat(COLUMN_PADDING));
            }

            if (isNumeric(value)) {
                // Right-align numeric columns
                sb.append(padLeft(truncated, widths[i]));
            } else {
                sb.append(padRight(truncated, widths[i]));
            }
        }
        return sb.toString().stripTrailing();
    }

    /**
     * Format a separator row using dashes.
     */
    private static String formatSeparator(int[] widths, int columnCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                sb.append(" ".repeat(COLUMN_PADDING));
            }
            sb.append("-".repeat(widths[i]));
        }
        return sb.toString().stripTrailing();
    }

    /**
     * Truncate a value to fit within the given width, appending an ellipsis if needed.
     */
    static String truncate(String value, int maxWidth) {
        if (value.length() <= maxWidth) {
            return value;
        }
        if (maxWidth <= TRUNCATION_SUFFIX.length()) {
            return value.substring(0, maxWidth);
        }
        return value.substring(0, maxWidth - TRUNCATION_SUFFIX.length()) + TRUNCATION_SUFFIX;
    }

    /**
     * Check if a string represents a numeric value.
     */
    static boolean isNumeric(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String padRight(String text, int width) {
        if (text.length() >= width) {
            return text;
        }
        return text + " ".repeat(width - text.length());
    }

    private static String padLeft(String text, int width) {
        if (text.length() >= width) {
            return text;
        }
        return " ".repeat(width - text.length()) + text;
    }

    private static String safeValue(String value) {
        return value == null ? "" : value;
    }

    private static int safeLength(String value) {
        return value == null ? 0 : value.length();
    }
}
