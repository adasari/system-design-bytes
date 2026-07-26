package io.minipinot.dict;

import io.minipinot.spec.DataType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * An immutable, sorted dictionary mapping dictionary ids ({@code 0 .. length-1}) to distinct
 * column values and back. Mirrors Pinot's immutable dictionaries (e.g. {@code IntDictionary},
 * {@code StringDictionary}) plus {@code SegmentDictionaryCreator}.
 *
 * <p>Layout: every entry occupies a fixed {@code stride} number of bytes so the dictionary is a
 * flat, binary-searchable array. Numeric strides equal the type width; STRING/BYTES entries are
 * padded with {@code \0} up to the longest entry (Pinot pads with
 * {@code V1Constants.Str.DEFAULT_STRING_PAD_CHAR}). Because {@code \0} sorts lowest, the padded
 * byte order matches lexicographic value order.
 */
public final class SortedDictionary {
  private final DataType _dataType;
  private final int _length;
  private final int _stride;
  private final ByteBuffer _buffer;
  private final int _baseOffset;

  public SortedDictionary(DataType dataType, int length, int stride, ByteBuffer buffer,
      int baseOffset) {
    _dataType = dataType;
    _length = length;
    _stride = stride;
    _buffer = buffer;
    _baseOffset = baseOffset;
  }

  public int length() {
    return _length;
  }

  public int getStride() {
    return _stride;
  }

  public DataType getDataType() {
    return _dataType;
  }

  /** Decode the value stored at {@code dictId}. */
  public Object get(int dictId) {
    int offset = _baseOffset + dictId * _stride;
    switch (_dataType) {
      case INT:
        return _buffer.getInt(offset);
      case LONG:
        return _buffer.getLong(offset);
      case FLOAT:
        return _buffer.getFloat(offset);
      case DOUBLE:
        return _buffer.getDouble(offset);
      case STRING:
        return new String(readPadded(offset), StandardCharsets.UTF_8);
      case BYTES:
        return readPadded(offset);
      default:
        throw new IllegalStateException("Unhandled type: " + _dataType);
    }
  }

  private byte[] readPadded(int offset) {
    int end = _stride;
    while (end > 0 && _buffer.get(offset + end - 1) == 0) {
      end--;
    }
    byte[] out = new byte[end];
    for (int i = 0; i < end; i++) {
      out[i] = _buffer.get(offset + i);
    }
    return out;
  }

  /**
   * Binary search for {@code value}. Returns its dictId if present, otherwise
   * {@code -(insertionPoint) - 1} (same convention as {@code Arrays.binarySearch} and Pinot),
   * which range indexes use to locate boundary dictIds.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public int indexOf(Object rawValue) {
    Comparable target = _dataType.toComparable(rawValue);
    int lo = 0;
    int hi = _length - 1;
    while (lo <= hi) {
      int mid = (lo + hi) / 2;
      int cmp = _dataType.toComparable(get(mid)).compareTo(target);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid - 1;
      } else {
        return mid;
      }
    }
    return -(lo) - 1;
  }

  // ---- Creation ----------------------------------------------------------

  /** The serialized bytes (length * stride) backing this dictionary. */
  public byte[] serialize() {
    byte[] out = new byte[_length * _stride];
    for (int i = 0; i < out.length; i++) {
      out[i] = _buffer.get(_baseOffset + i);
    }
    return out;
  }

  /** Build a dictionary from the sorted, distinct values produced by the stats pass. */
  public static SortedDictionary build(DataType dataType, Comparable<?>[] sortedUniqueValues) {
    int length = sortedUniqueValues.length;
    int stride = computeStride(dataType, sortedUniqueValues);
    ByteBuffer buffer = ByteBuffer.allocate(Math.max(1, length * stride));
    for (int i = 0; i < length; i++) {
      writeEntry(buffer, i * stride, dataType, sortedUniqueValues[i], stride);
    }
    return new SortedDictionary(dataType, length, stride, buffer, 0);
  }

  private static int computeStride(DataType dataType, Comparable<?>[] values) {
    if (dataType.isFixedWidth()) {
      return dataType.getSizeInBytes();
    }
    int max = 1;
    for (Comparable<?> value : values) {
      max = Math.max(max, encodeVarBytes(dataType, value).length);
    }
    return max;
  }

  private static void writeEntry(ByteBuffer buffer, int offset, DataType dataType, Object value,
      int stride) {
    switch (dataType) {
      case INT:
        buffer.putInt(offset, (Integer) value);
        break;
      case LONG:
        buffer.putLong(offset, (Long) value);
        break;
      case FLOAT:
        buffer.putFloat(offset, (Float) value);
        break;
      case DOUBLE:
        buffer.putDouble(offset, (Double) value);
        break;
      case STRING:
      case BYTES:
        byte[] bytes = encodeVarBytes(dataType, value);
        for (int i = 0; i < bytes.length; i++) {
          buffer.put(offset + i, bytes[i]);
        }
        // remaining bytes stay 0 (pad char)
        break;
      default:
        throw new IllegalStateException("Unhandled type: " + dataType);
    }
  }

  private static byte[] encodeVarBytes(DataType dataType, Object value) {
    if (dataType == DataType.STRING) {
      return value.toString().getBytes(StandardCharsets.UTF_8);
    }
    // BYTES values arrive as ISO-8859-1 String proxies from the stats pass; round-trip them back.
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    return value.toString().getBytes(StandardCharsets.ISO_8859_1);
  }
}
