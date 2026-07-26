package io.minipinot.store;

import io.minipinot.forward.ForwardIndexEncoding;
import java.util.HashSet;
import java.util.Set;

/**
 * Declares which optional indexes to build per column, analogous to the index configuration in a
 * Pinot table config. Forward index + dictionary are always built; inverted/range/bloom are opt-in.
 */
public final class SegmentBuildConfig {
  private final Set<String> _invertedIndexColumns = new HashSet<>();
  private final Set<String> _rangeIndexColumns = new HashSet<>();
  private final Set<String> _bloomFilterColumns = new HashSet<>();
  private ForwardIndexEncoding _forwardIndexEncoding = ForwardIndexEncoding.HANDCRAFTED;

  public SegmentBuildConfig withInvertedIndex(String... columns) {
    for (String c : columns) {
      _invertedIndexColumns.add(c);
    }
    return this;
  }

  public SegmentBuildConfig withRangeIndex(String... columns) {
    for (String c : columns) {
      _rangeIndexColumns.add(c);
    }
    return this;
  }

  public SegmentBuildConfig withBloomFilter(String... columns) {
    for (String c : columns) {
      _bloomFilterColumns.add(c);
    }
    return this;
  }

  /** Choose the forward-index packing implementation (defaults to {@code HANDCRAFTED}). */
  public SegmentBuildConfig withForwardIndexEncoding(ForwardIndexEncoding encoding) {
    _forwardIndexEncoding = encoding;
    return this;
  }

  public ForwardIndexEncoding getForwardIndexEncoding() {
    return _forwardIndexEncoding;
  }

  public boolean hasInvertedIndex(String column) {
    return _invertedIndexColumns.contains(column);
  }

  public boolean hasRangeIndex(String column) {
    return _rangeIndexColumns.contains(column);
  }

  public boolean hasBloomFilter(String column) {
    return _bloomFilterColumns.contains(column);
  }
}
