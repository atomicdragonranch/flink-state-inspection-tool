package io.flinkstate.inspector.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TableFormatterTest {

    @Test
    void formatTableWithSimpleData() {
        // Arrange
        List<String> headers = List.of("NAME", "AGE", "CITY");
        List<List<String>> rows = List.of(
            List.of("Alice", "30", "New York"),
            List.of("Bob", "25", "Boston")
        );

        // Act
        String result = TableFormatter.formatTable(headers, rows);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        assertThat(lines).hasSizeGreaterThanOrEqualTo(4); // header + separator + 2 data rows
        assertThat(lines[0]).contains("NAME").contains("AGE").contains("CITY");
        assertThat(lines[1]).matches("^-+\\s+-+\\s+-+$");
        assertThat(lines[2]).contains("Alice").contains("New York");
        assertThat(lines[3]).contains("Bob").contains("Boston");
    }

    @Test
    void formatTableRightAlignsNumericValues() {
        // Arrange
        List<String> headers = List.of("ITEM", "COUNT");
        List<List<String>> rows = List.of(
            List.of("apples", "42"),
            List.of("oranges", "7")
        );

        // Act
        String result = TableFormatter.formatTable(headers, rows);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        // "42" should be right-aligned, meaning it has leading spaces
        String dataLine1 = lines[2];
        String dataLine2 = lines[3];
        // The numeric column value "7" should have more leading space than "42"
        int pos42 = dataLine1.indexOf("42");
        int pos7 = dataLine2.indexOf("7");
        // Both should appear at the same right-edge position
        // "42" is 2 chars, " 7" would also end at the same spot
        assertThat(pos42 + 2).isEqualTo(pos7 + 1);
    }

    @Test
    void formatTableTruncatesLongValues() {
        // Arrange
        List<String> headers = List.of("NAME", "DESCRIPTION");
        String longValue = "A".repeat(100);
        List<List<String>> rows = List.of(
            List.of("test", longValue)
        );
        int maxWidth = 20;

        // Act
        String result = TableFormatter.formatTable(headers, rows, maxWidth);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        String dataLine = lines[2];
        // The long value should be truncated with ellipsis
        assertThat(dataLine).contains("...");
        // No single token should exceed maxWidth
        assertThat(dataLine).doesNotContain("A".repeat(maxWidth + 1));
    }

    @Test
    void formatTableWithEmptyRows() {
        // Arrange
        List<String> headers = List.of("TYPE", "VALUE");
        List<List<String>> rows = Collections.emptyList();

        // Act
        String result = TableFormatter.formatTable(headers, rows);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        assertThat(lines).hasSize(2); // header + separator only
        assertThat(lines[0]).contains("TYPE").contains("VALUE");
        assertThat(lines[1]).matches("^-+\\s+-+$");
    }

    @Test
    void formatTableWithNullHeaders() {
        // Arrange
        List<String> headers = null;

        // Act
        String result = TableFormatter.formatTable(headers, Collections.emptyList());

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    void formatTableWithEmptyHeaders() {
        // Arrange
        List<String> headers = Collections.emptyList();

        // Act
        String result = TableFormatter.formatTable(headers, Collections.emptyList());

        // Assert
        assertThat(result).isEmpty();
    }

    @Test
    void formatTableWithNullCellValues() {
        // Arrange
        List<String> headers = List.of("A", "B");
        List<List<String>> rows = List.of(
            Arrays.asList("value", null)
        );

        // Act
        String result = TableFormatter.formatTable(headers, rows);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        assertThat(lines).hasSize(3);
        assertThat(lines[2]).contains("value");
    }

    @Test
    void truncateShortValueUnchanged() {
        // Arrange
        String value = "hello";
        int maxWidth = 10;

        // Act
        String result = TableFormatter.truncate(value, maxWidth);

        // Assert
        assertThat(result).isEqualTo("hello");
    }

    @Test
    void truncateLongValueWithEllipsis() {
        // Arrange
        String value = "this is a very long string";
        int maxWidth = 10;

        // Act
        String result = TableFormatter.truncate(value, maxWidth);

        // Assert
        assertThat(result).hasSize(10);
        assertThat(result).endsWith(TableFormatter.TRUNCATION_SUFFIX);
    }

    @Test
    void truncateWithVerySmallMaxWidth() {
        // Arrange
        String value = "hello";
        int maxWidth = 2;

        // Act
        String result = TableFormatter.truncate(value, maxWidth);

        // Assert
        assertThat(result).hasSize(2);
        assertThat(result).isEqualTo("he");
    }

    @Test
    void isNumericWithIntegers() {
        // Arrange / Act / Assert
        assertThat(TableFormatter.isNumeric("42")).isTrue();
        assertThat(TableFormatter.isNumeric("-7")).isTrue();
        assertThat(TableFormatter.isNumeric("0")).isTrue();
    }

    @Test
    void isNumericWithDecimals() {
        // Arrange / Act / Assert
        assertThat(TableFormatter.isNumeric("3.14")).isTrue();
        assertThat(TableFormatter.isNumeric("-0.5")).isTrue();
    }

    @Test
    void isNumericWithNonNumericValues() {
        // Arrange / Act / Assert
        assertThat(TableFormatter.isNumeric("hello")).isFalse();
        assertThat(TableFormatter.isNumeric("")).isFalse();
        assertThat(TableFormatter.isNumeric(null)).isFalse();
        assertThat(TableFormatter.isNumeric("12abc")).isFalse();
    }

    @Test
    void formatTableColumnWidthsRespectMinimum() {
        // Arrange
        List<String> headers = List.of("A", "B");
        List<List<String>> rows = List.of(
            List.of("x", "y")
        );

        // Act
        String result = TableFormatter.formatTable(headers, rows);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        // Separator dashes should be at least MIN_COLUMN_WIDTH long
        String separator = lines[1];
        String[] parts = separator.split("\\s+");
        for (String part : parts) {
            assertThat(part.length()).isGreaterThanOrEqualTo(TableFormatter.MIN_COLUMN_WIDTH);
        }
    }

    @Test
    void formatTableWithRowsShorterThanHeaders() {
        // Arrange
        List<String> headers = List.of("A", "B", "C");
        List<List<String>> rows = List.of(
            List.of("only-one")
        );

        // Act
        String result = TableFormatter.formatTable(headers, rows);

        // Assert
        String[] lines = result.split(System.lineSeparator());
        assertThat(lines).hasSize(3);
        assertThat(lines[2]).contains("only-one");
    }
}
