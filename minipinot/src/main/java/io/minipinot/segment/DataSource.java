package io.minipinot.segment;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.invert.BitmapInvertedIndex;
import io.minipinot.invert.BloomFilter;
import io.minipinot.invert.RangeIndex;
import io.minipinot.store.ColumnMetadata;

/**
 * The per-column query API. Mirrors Pinot's {@code org.apache.pinot.segment.spi.datasource.DataSource}:
 * it bundles the column's metadata with its physical index readers so filter/projection operators
 * can pick the cheapest access path (inverted / range / sorted / scan).
 *
 * <p>Accessors return {@code null} when the corresponding index was not built for the column, which
 * is exactly how the plan maker decides whether an indexed path is available.
 */
public interface DataSource {

  ColumnMetadata getDataSourceMetadata();

  SortedDictionary getDictionary();

  ForwardIndexReader getForwardIndex();

  /** Non-null only if an inverted index was built for the column. */
  BitmapInvertedIndex.Reader getInvertedIndex();

  /** Non-null only if a range index was built for the column. */
  RangeIndex.Reader getRangeIndex();

  /** Non-null only if a bloom filter was built for the column. */
  BloomFilter.Reader getBloomFilter();
}
