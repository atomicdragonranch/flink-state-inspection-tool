package io.flinkstate.inspector.api;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ApiResponse<T> {

    private T data;
    private Boolean partialRead;
    private String partialReadCause;
    private String error;
    private String stackTrace;

    private ApiResponse() {
    }

    private ApiResponse(T data, Boolean partialRead, String partialReadCause,
                        String error, String stackTrace) {
        this.data = data;
        this.partialRead = partialRead;
        this.partialReadCause = partialReadCause;
        this.error = error;
        this.stackTrace = stackTrace;
    }

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(data, null, null, null, null);
    }

    public static <T> ApiResponse<T> successWithPartialRead(T data, String cause) {
        return new ApiResponse<>(data, true, cause, null, null);
    }

    public static <T> ApiResponse<T> error(String message) {
        return new ApiResponse<>(null, null, null, message, null);
    }

    public static <T> ApiResponse<T> error(String message, String stackTrace) {
        return new ApiResponse<>(null, null, null, message, stackTrace);
    }

    public T getData() { return data; }
    public Boolean getPartialRead() { return partialRead; }
    public String getPartialReadCause() { return partialReadCause; }
    public String getError() { return error; }
    public String getStackTrace() { return stackTrace; }
}
