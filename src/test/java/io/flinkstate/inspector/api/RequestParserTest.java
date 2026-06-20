package io.flinkstate.inspector.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestParserTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonNode json(String raw) throws Exception {
        return MAPPER.readTree(raw);
    }

    @Test
    void requireFieldReturnsValueWhenPresent() throws Exception {
        // Arrange
        JsonNode body = json("{\"name\": \"alice\"}");

        // Act
        String result = RequestParser.requireField(body, "name");

        // Assert
        assertThat(result).isEqualTo("alice");
    }

    @Test
    void requireFieldThrowsWhenMissing() throws Exception {
        // Arrange
        JsonNode body = json("{\"other\": \"value\"}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.requireField(body, "name"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required field: name");
    }

    @Test
    void requireFieldThrowsWhenEmpty() throws Exception {
        // Arrange
        JsonNode body = json("{\"name\": \"\"}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.requireField(body, "name"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing required field: name");
    }

    @Test
    void intFieldReturnsDefaultWhenAbsent() throws Exception {
        // Arrange
        JsonNode body = json("{\"other\": 5}");

        // Act
        int result = RequestParser.intField(body, "limit", 100);

        // Assert
        assertThat(result).isEqualTo(100);
    }

    @Test
    void intFieldReturnsProvidedValue() throws Exception {
        // Arrange
        JsonNode body = json("{\"limit\": 500}");

        // Act
        int result = RequestParser.intField(body, "limit", 100);

        // Assert
        assertThat(result).isEqualTo(500);
    }

    @Test
    void intFieldThrowsWhenBelowMinimum() throws Exception {
        // Arrange
        JsonNode body = json("{\"limit\": 0}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.intField(body, "limit", 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("limit must be between 1 and " + RequestParser.MAX_LIMIT);
    }

    @Test
    void intFieldThrowsWhenAboveMaximum() throws Exception {
        // Arrange
        JsonNode body = json("{\"limit\": 200000}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.intField(body, "limit", 100))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("limit must be between 1 and " + RequestParser.MAX_LIMIT);
    }

    @Test
    void intFieldThrowsWhenDefaultOutOfRange() throws Exception {
        // Arrange
        JsonNode body = json("{}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.intField(body, "limit", -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("limit must be between 1 and " + RequestParser.MAX_LIMIT);
    }

    @Test
    void optionalFieldReturnsValueWhenPresent() throws Exception {
        // Arrange
        JsonNode body = json("{\"filter\": \"abc\"}");

        // Act
        String result = RequestParser.optionalField(body, "filter");

        // Assert
        assertThat(result).isEqualTo("abc");
    }

    @Test
    void optionalFieldReturnsNullWhenAbsent() throws Exception {
        // Arrange
        JsonNode body = json("{\"other\": \"value\"}");

        // Act
        String result = RequestParser.optionalField(body, "filter");

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void optionalFieldReturnsNullWhenNull() throws Exception {
        // Arrange
        JsonNode body = json("{\"filter\": null}");

        // Act
        String result = RequestParser.optionalField(body, "filter");

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void optionalFieldReturnsNullWhenEmpty() throws Exception {
        // Arrange
        JsonNode body = json("{\"filter\": \"\"}");

        // Act
        String result = RequestParser.optionalField(body, "filter");

        // Assert
        assertThat(result).isNull();
    }

    @Test
    void boolFieldReturnsTrueWhenTrue() throws Exception {
        // Arrange
        JsonNode body = json("{\"keysOnly\": true}");

        // Act
        boolean result = RequestParser.boolField(body, "keysOnly");

        // Assert
        assertThat(result).isTrue();
    }

    @Test
    void boolFieldReturnsFalseWhenAbsent() throws Exception {
        // Arrange
        JsonNode body = json("{\"other\": true}");

        // Act
        boolean result = RequestParser.boolField(body, "other_field");

        // Assert
        assertThat(result).isFalse();
    }

    @Test
    void intFieldAllowZeroReturnsZeroDefault() throws Exception {
        // Arrange
        JsonNode body = json("{}");

        // Act
        int result = RequestParser.intFieldAllowZero(body, "offset", 0);

        // Assert
        assertThat(result).isEqualTo(0);
    }

    @Test
    void intFieldAllowZeroAcceptsZeroValue() throws Exception {
        // Arrange
        JsonNode body = json("{\"offset\": 0}");

        // Act
        int result = RequestParser.intFieldAllowZero(body, "offset", 5);

        // Assert
        assertThat(result).isEqualTo(0);
    }

    @Test
    void intFieldAllowZeroAcceptsPositiveValue() throws Exception {
        // Arrange
        JsonNode body = json("{\"offset\": 50}");

        // Act
        int result = RequestParser.intFieldAllowZero(body, "offset", 0);

        // Assert
        assertThat(result).isEqualTo(50);
    }

    @Test
    void intFieldAllowZeroThrowsWhenNegative() throws Exception {
        // Arrange
        JsonNode body = json("{\"offset\": -1}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.intFieldAllowZero(body, "offset", 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("offset must be between 0 and " + RequestParser.MAX_LIMIT);
    }

    @Test
    void intFieldAllowZeroThrowsWhenAboveMaximum() throws Exception {
        // Arrange
        JsonNode body = json("{\"offset\": 200000}");

        // Act / Assert
        assertThatThrownBy(() -> RequestParser.intFieldAllowZero(body, "offset", 0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("offset must be between 0 and " + RequestParser.MAX_LIMIT);
    }
}
