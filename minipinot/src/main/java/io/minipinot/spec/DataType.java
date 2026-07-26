package io.minipinot.spec;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * The physical storage type of a column value. Mirrors the subset of
 * {@code org.apache.pinot.spi.data.FieldSpec.DataType} that MiniPinot supports.
 *
 * <p>Each type knows how to parse a raw string token, produce a default null value
 * and (for fixed-width numeric types) report its byte size. Variable-width types
 * (STRING, BYTES) report a size of -1.
 */
public enum DataType {
  INT(4),
  LONG(8),
  FLOAT(4),
  DOUBLE(8),
  STRING(-1),
  BYTES(-1);

  private final int _sizeInBytes;

  DataType(int sizeInBytes) {
    _sizeInBytes = sizeInBytes;
  }

  /** Fixed byte width, or -1 for variable-length types. */
  public int getSizeInBytes() {
    return _sizeInBytes;
  }

  public boolean isFixedWidth() {
    return _sizeInBytes > 0;
  }

  public boolean isNumeric() {
    return this == INT || this == LONG || this == FLOAT || this == DOUBLE;
  }

  /** Parse a raw CSV/JSON string token into the Java value for this type. */
  public Object parse(String token) {
    if (token == null) {
      return null;
    }
    switch (this) {
      case INT:
        return Integer.parseInt(token.trim());
      case LONG:
        return Long.parseLong(token.trim());
      case FLOAT:
        return Float.parseFloat(token.trim());
      case DOUBLE:
        return Double.parseDouble(token.trim());
      case STRING:
        return token;
      case BYTES:
        return token.getBytes(StandardCharsets.UTF_8);
      default:
        throw new IllegalStateException("Unhandled data type: " + this);
    }
  }

  /** The default null value used when a column value is missing. Matches Pinot defaults. */
  public Object getDefaultNullValue() {
    switch (this) {
      case INT:
        return Integer.MIN_VALUE;
      case LONG:
        return Long.MIN_VALUE;
      case FLOAT:
        return Float.NEGATIVE_INFINITY;
      case DOUBLE:
        return Double.NEGATIVE_INFINITY;
      case STRING:
        return "null";
      case BYTES:
        return new byte[0];
      default:
        throw new IllegalStateException("Unhandled data type: " + this);
    }
  }

  /** Convert a value of this type to a comparable form (used by stats/dictionary). */
  @SuppressWarnings("unchecked")
  public Comparable<Object> toComparable(Object value) {
    if (this == BYTES) {
      // Compare bytes lexicographically via a stable string proxy.
      return (Comparable<Object>) (Comparable<?>) new String((byte[]) value, StandardCharsets.ISO_8859_1);
    }
    return (Comparable<Object>) value;
  }

  public static BigDecimal toBigDecimal(Object numericValue) {
    return new BigDecimal(String.valueOf(numericValue));
  }
}
