package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Shared helpers for extracting typed fields from JSON request bodies.
 * All endpoint classes delegate here instead of duplicating parse logic.
 */
public final class RequestParser {

    static final int MAX_LIMIT = 100_000;

    private RequestParser() {
    }

    /**
     * Returns the text value of a required field, or throws if missing/empty.
     */
    public static String requireField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.asText().isEmpty()) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return node.asText();
    }

    /**
     * Returns the text value of an optional field, or {@code null} if absent/empty.
     */
    public static String optionalField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        if (node == null || node.isNull() || node.asText().isEmpty()) {
            return null;
        }
        return node.asText();
    }

    /**
     * Returns the boolean value of a field, defaulting to {@code false} if absent.
     */
    public static boolean boolField(JsonNode body, String fieldName) {
        JsonNode node = body.get(fieldName);
        return node != null && node.asBoolean();
    }

    /**
     * Returns the int value of a field, using {@code defaultValue} when absent.
     * The returned value must be between 1 and {@link #MAX_LIMIT} (inclusive).
     */
    public static int intField(JsonNode body, String fieldName, int defaultValue) {
        JsonNode node = body.get(fieldName);
        int value = node != null && node.isNumber() ? node.asInt() : defaultValue;
        if (value < 1 || value > MAX_LIMIT) {
            throw new IllegalArgumentException(
                fieldName + " must be between 1 and " + MAX_LIMIT);
        }
        return value;
    }

    /**
     * Returns the int value of a field, using {@code defaultValue} when absent.
     * The returned value must be between 0 and {@link #MAX_LIMIT} (inclusive).
     * Unlike {@link #intField}, this method allows zero as a valid value,
     * making it suitable for offset parameters.
     */
    public static int intFieldAllowZero(JsonNode body, String fieldName, int defaultValue) {
        JsonNode node = body.get(fieldName);
        int value = node != null && node.isNumber() ? node.asInt() : defaultValue;
        if (value < 0 || value > MAX_LIMIT) {
            throw new IllegalArgumentException(
                fieldName + " must be between 0 and " + MAX_LIMIT);
        }
        return value;
    }
}
