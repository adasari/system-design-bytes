package io.minipinot.store;

import java.util.Objects;

/**
 * Identifies one buffer in the segment: a (column, {@link IndexType}) pair. Used as the key in the
 * single-file store's {@code index_map}. Mirrors Pinot's {@code IndexKey}.
 */
public final class IndexKey {
  private final String _column;
  private final IndexType _indexType;

  public IndexKey(String column, IndexType indexType) {
    _column = column;
    _indexType = indexType;
  }

  public String getColumn() {
    return _column;
  }

  public IndexType getIndexType() {
    return _indexType;
  }

  /** The {@code index_map} property prefix: {@code <column>.<indexTypeId>}. */
  public String toPropertyPrefix() {
    return _column + "." + _indexType.getId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof IndexKey)) {
      return false;
    }
    IndexKey other = (IndexKey) o;
    return _column.equals(other._column) && _indexType == other._indexType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_column, _indexType);
  }

  @Override
  public String toString() {
    return toPropertyPrefix();
  }
}
