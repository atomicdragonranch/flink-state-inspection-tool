package io.flinkstate.inspector.reader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses binary data into structured fields by detecting strings, integers, and longs
 * within serialized operator state values.
 */
final class StructuredFieldParser {

    private StructuredFieldParser() {
    }

    static Map<String, Object> parseStructuredFields(byte[] data) {
        if (data.length < 4) return null;

        Map<String, Object> fields = new LinkedHashMap<>();
        int pos = 0;
        int fieldNum = 0;

        try {
            while (pos < data.length) {
                int remaining = data.length - pos;

                if (remaining >= 2) {
                    int strLen = ((data[pos] & 0xFF) << 8) | (data[pos + 1] & 0xFF);
                    if (strLen > 0 && strLen <= remaining - 2 && isUtf8String(data, pos + 2, strLen)) {
                        String s = new String(data, pos + 2, strLen, StandardCharsets.UTF_8);
                        fields.put("field_" + fieldNum++, s);
                        pos += 2 + strLen;
                        continue;
                    }
                }

                if (remaining >= 8) {
                    long longVal = readLong(data, pos);
                    if (remaining >= 12) {
                        int nextStrLen = -1;
                        if (pos + 8 + 2 <= data.length) {
                            nextStrLen = ((data[pos + 8] & 0xFF) << 8) | (data[pos + 8 + 1] & 0xFF);
                        }
                        if (nextStrLen > 0 && pos + 8 + 2 + nextStrLen <= data.length
                                && isUtf8String(data, pos + 10, nextStrLen)) {
                            fields.put("field_" + fieldNum++, longVal);
                            pos += 8;
                            continue;
                        }
                    }

                    if (remaining >= 4) {
                        int intVal = readInt(data, pos);
                        if (intVal >= -1_000_000 && intVal <= 10_000_000 && remaining >= 8) {
                            fields.put("field_" + fieldNum++, intVal);
                            pos += 4;
                            continue;
                        }

                        if (longVal >= -1_000_000_000_000L && longVal <= 10_000_000_000_000L) {
                            fields.put("field_" + fieldNum++, longVal);
                            pos += 8;
                            continue;
                        }

                        fields.put("field_" + fieldNum++, intVal);
                        pos += 4;
                        continue;
                    }
                }

                if (remaining >= 4) {
                    fields.put("field_" + fieldNum++, readInt(data, pos));
                    pos += 4;
                    continue;
                }

                fields.put("field_" + fieldNum++,
                    GenericStateReader.bytesToHex(
                        Arrays.copyOfRange(data, pos, data.length), 256));
                break;
            }
        } catch (Exception e) {
            return null;
        }

        if (fields.size() < 2) return null;
        return fields;
    }

    static boolean isUtf8String(byte[] data, int offset, int len) {
        if (offset + len > data.length) return false;
        int printable = 0;
        for (int i = 0; i < len; i++) {
            int b = data[offset + i] & 0xFF;
            if (b >= 0x20 && b <= 0x7E) {
                printable++;
            } else if (b == 0x09 || b == 0x0A || b == 0x0D) {
                // tabs and newlines are ok
            } else if (b >= 0xC0 && b <= 0xF7) {
                // multi-byte UTF-8 lead byte, ok
            } else if (b >= 0x80 && b <= 0xBF) {
                // UTF-8 continuation byte, ok
            } else {
                return false;
            }
        }
        return printable > len / 2;
    }

    static int readInt(byte[] data, int pos) {
        return ((data[pos] & 0xFF) << 24)
             | ((data[pos + 1] & 0xFF) << 16)
             | ((data[pos + 2] & 0xFF) << 8)
             | (data[pos + 3] & 0xFF);
    }

    static long readLong(byte[] data, int pos) {
        return ((long)(data[pos] & 0xFF) << 56)
             | ((long)(data[pos + 1] & 0xFF) << 48)
             | ((long)(data[pos + 2] & 0xFF) << 40)
             | ((long)(data[pos + 3] & 0xFF) << 32)
             | ((long)(data[pos + 4] & 0xFF) << 24)
             | ((long)(data[pos + 5] & 0xFF) << 16)
             | ((long)(data[pos + 6] & 0xFF) << 8)
             | (long)(data[pos + 7] & 0xFF);
    }
}
