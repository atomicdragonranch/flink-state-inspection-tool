package io.flinkstate.inspector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ObjectInputFilter;

import static org.assertj.core.api.Assertions.assertThat;

class DeserializationFilterTest {

    @BeforeAll
    static void installFilter() {
        FlinkStateInspector.installDeserializationFilter();
    }

    @Test
    void filterIsInstalledAfterSetup() {
        // Arrange
        // Filter was installed in @BeforeAll

        // Act
        ObjectInputFilter filter = ObjectInputFilter.Config.getSerialFilter();

        // Assert
        assertThat(filter).isNotNull();
    }

    @Test
    void filterAcceptsFlinkClasses() {
        // Arrange
        ObjectInputFilter filter = ObjectInputFilter.Config.getSerialFilter();
        ObjectInputFilter.FilterInfo flinkInfo = createFilterInfo(
                org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata.class);

        // Act
        ObjectInputFilter.Status status = filter.checkInput(flinkInfo);

        // Assert
        assertThat(status).isEqualTo(ObjectInputFilter.Status.ALLOWED);
    }

    @Test
    void filterAcceptsJavaLangClasses() {
        // Arrange
        ObjectInputFilter filter = ObjectInputFilter.Config.getSerialFilter();
        ObjectInputFilter.FilterInfo javaInfo = createFilterInfo(java.util.HashMap.class);

        // Act
        ObjectInputFilter.Status status = filter.checkInput(javaInfo);

        // Assert
        assertThat(status).isEqualTo(ObjectInputFilter.Status.ALLOWED);
    }

    @Test
    void filterRejectsArbitraryClasses() {
        // Arrange
        ObjectInputFilter filter = ObjectInputFilter.Config.getSerialFilter();
        ObjectInputFilter.FilterInfo maliciousInfo = createFilterInfo(
                javax.management.remote.rmi.RMIConnector.class);

        // Act
        ObjectInputFilter.Status status = filter.checkInput(maliciousInfo);

        // Assert
        assertThat(status).isEqualTo(ObjectInputFilter.Status.REJECTED);
    }

    @Test
    void filterPatternMatchesExpectedValue() {
        // Arrange
        String expectedPattern = "org.apache.flink.**;java.**;!*";

        // Act
        String actualPattern = FlinkStateInspector.DESERIALIZATION_FILTER_PATTERN;

        // Assert
        assertThat(actualPattern).isEqualTo(expectedPattern);
    }

    /**
     * Creates a minimal FilterInfo for testing the filter against a specific class.
     */
    private static ObjectInputFilter.FilterInfo createFilterInfo(Class<?> clazz) {
        return new ObjectInputFilter.FilterInfo() {
            @Override
            public Class<?> serialClass() {
                return clazz;
            }

            @Override
            public long arrayLength() {
                return -1;
            }

            @Override
            public long depth() {
                return 1;
            }

            @Override
            public long references() {
                return 1;
            }

            @Override
            public long streamBytes() {
                return 0;
            }
        };
    }
}
