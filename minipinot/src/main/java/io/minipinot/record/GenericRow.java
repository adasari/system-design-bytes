package io.minipinot.record;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A single decoded input row: a mapping from column name to its typed Java value.
 * Mirrors {@code org.apache.pinot.spi.data.readers.GenericRow}. For single-valued columns
 * the value is a boxed scalar (Integer/Long/Float/Double/String/byte[]); for multi-valued
 * columns it is an Object[].
 */
public final class GenericRow {
  private final Map<String, Object> _values = new LinkedHashMap<>();

  public void putValue(String column, Object value) {
    _values.put(column, value);
  }

  public Object getValue(String column) {
    return _values.get(column);
  }

  public boolean isNullValue(String column) {
    return _values.get(column) == null;
  }

  public Set<String> getColumnNames() {
    return _values.keySet();
  }

  public void clear() {
    _values.clear();
  }

  @Override
  public String toString() {
    return _values.toString();
  }
}
