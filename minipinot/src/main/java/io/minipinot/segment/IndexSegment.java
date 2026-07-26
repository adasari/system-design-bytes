package io.minipinot.segment;

import io.minipinot.store.SegmentMetadata;
import java.util.Collection;

/**
 * An immutable, queryable segment loaded into memory (its indexes memory-mapped). This is the read
 * side counterpart of the write path built in phases 1-3, and mirrors Pinot's
 * {@code org.apache.pinot.segment.spi.IndexSegment} / {@code ImmutableSegment}.
 *
 * <p>The query engine never reads raw files: it goes through {@link #getDataSource(String)} to get a
 * per-column {@link DataSource} exposing that column's dictionary, forward index and any inverted /
 * range / bloom indexes.
 */
public interface IndexSegment extends AutoCloseable {

  String getSegmentName();

  SegmentMetadata getSegmentMetadata();

  int getTotalDocCount();

  Collection<String> getColumnNames();

  /** Per-column query API (readers + metadata). Throws if the column is unknown. */
  DataSource getDataSource(String column);

  @Override
  void close();
}
