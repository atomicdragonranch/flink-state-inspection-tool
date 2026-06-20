package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ApiResponseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void successResponseContainsDataField() throws Exception {
        // Arrange
        ApiResponse<?> response = ApiResponse.success(Map.of("key", "value"));

        // Act
        String json = MAPPER.writeValueAsString(response);

        // Assert
        assertThat(json).contains("\"data\":{\"key\":\"value\"}");
        assertThat(json).doesNotContain("\"error\"");
    }

    @Test
    void successResponseWithListData() throws Exception {
        // Arrange
        ApiResponse<?> response = ApiResponse.success(List.of("a", "b"));

        // Act
        String json = MAPPER.writeValueAsString(response);

        // Assert
        assertThat(json).contains("\"data\":[\"a\",\"b\"]");
    }

    @Test
    void errorResponseContainsErrorField() throws Exception {
        // Arrange
        ApiResponse<?> response = ApiResponse.error("something broke");

        // Act
        String json = MAPPER.writeValueAsString(response);

        // Assert
        assertThat(json).contains("\"error\":\"something broke\"");
    }

    @Test
    void errorResponseDoesNotContainStackTrace() throws Exception {
        // Arrange
        ApiResponse<?> response = ApiResponse.error("msg");

        // Act
        String json = MAPPER.writeValueAsString(response);

        // Assert
        assertThat(json).contains("\"error\":\"msg\"");
        assertThat(json).doesNotContain("stackTrace");
    }
}
