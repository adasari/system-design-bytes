package io.minipinot.store;

/**
 * The kinds of per-column buffers a segment can persist. Each (column, IndexType) pair becomes one
 * entry in the single index file ({@code columns.psf}) and one record in the {@code index_map}.
 * Mirrors the role of Pinot's {@code StandardIndexes} / {@code ColumnIndexType}.
 */
public enum IndexType {
  DICTIONARY("dictionary"),
  FORWARD_INDEX("forward_index"),
  INVERTED_INDEX("inverted_index"),
  RANGE_INDEX("range_index"),
  BLOOM_FILTER("bloom_filter");

  private final String _id;

  IndexType(String id) {
    _id = id;
  }

  public String getId() {
    return _id;
  }

  public static IndexType fromId(String id) {
    for (IndexType type : values()) {
      if (type._id.equals(id)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown index type id: " + id);
  }
}
