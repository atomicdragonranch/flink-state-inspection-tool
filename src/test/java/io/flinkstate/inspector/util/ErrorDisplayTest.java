package io.flinkstate.inspector.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorDisplayTest {

    @Test
    void extractRootCauseWithNoCause() {
        // Arrange
        Exception e = new IllegalArgumentException("bad input");

        // Act
        String result = ErrorDisplay.extractRootCause(e);

        // Assert
        assertThat(result).isEqualTo("IllegalArgumentException: bad input");
    }

    @Test
    void extractRootCauseWalksChain() {
        // Arrange
        Exception root = new NullPointerException("the real problem");
        Exception mid = new RuntimeException("wrapper", root);
        Exception top = new Exception("top level", mid);

        // Act
        String result = ErrorDisplay.extractRootCause(top);

        // Assert
        assertThat(result).isEqualTo("NullPointerException: the real problem");
    }

    @Test
    void extractRootCauseWithNullMessage() {
        // Arrange
        Exception e = new RuntimeException((String) null);

        // Act
        String result = ErrorDisplay.extractRootCause(e);

        // Assert
        assertThat(result).isEqualTo("RuntimeException: null");
    }

    @Test
    void getStackTraceContainsClassName() {
        // Arrange
        Exception e = new IllegalStateException("test error");

        // Act
        String trace = ErrorDisplay.getStackTrace(e);

        // Assert
        assertThat(trace).contains("IllegalStateException");
        assertThat(trace).contains("test error");
        assertThat(trace).contains("ErrorDisplayTest");
    }
}
