package io.minipinot.segment;

import io.minipinot.store.SegmentDirectory;
import io.minipinot.store.SegmentMetadata;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link IndexSegment} backed by a memory-mapped {@link SegmentDirectory}. Data sources are built
 * eagerly (they only wrap mapped buffers, no data is copied). Mirrors Pinot's
 * {@code ImmutableSegmentImpl}.
 */
public final class ImmutableSegmentImpl implements IndexSegment {
  private final SegmentDirectory _segmentDirectory;
  private final SegmentMetadata _segmentMetadata;
  private final Map<String, DataSource> _dataSources;

  public ImmutableSegmentImpl(SegmentDirectory segmentDirectory) {
    _segmentDirectory = segmentDirectory;
    _segmentMetadata = segmentDirectory.getMetadata();
    _dataSources = new LinkedHashMap<>();
    for (String column : _segmentMetadata.getColumnNames()) {
      _dataSources.put(column, new ImmutableDataSource(segmentDirectory,
          _segmentMetadata.getColumnMetadata(column), _segmentMetadata.getForwardIndexEncoding()));
    }
  }

  @Override
  public String getSegmentName() {
    return _segmentMetadata.getSegmentName();
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public int getTotalDocCount() {
    return _segmentMetadata.getTotalDocs();
  }

  @Override
  public Collection<String> getColumnNames() {
    return _dataSources.keySet();
  }

  @Override
  public DataSource getDataSource(String column) {
    DataSource dataSource = _dataSources.get(column);
    if (dataSource == null) {
      throw new IllegalArgumentException("Unknown column: " + column);
    }
    return dataSource;
  }

  @Override
  public void close() {
    _segmentDirectory.close();
  }
}
