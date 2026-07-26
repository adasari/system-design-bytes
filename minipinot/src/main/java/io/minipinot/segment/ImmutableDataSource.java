package io.minipinot.segment;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.FixedBitForwardIndex;
import io.minipinot.forward.ForwardIndexEncoding;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.forward.LuceneFixedBitForwardIndex;
import io.minipinot.forward.SortedForwardIndex;
import io.minipinot.invert.BitmapInvertedIndex;
import io.minipinot.invert.BloomFilter;
import io.minipinot.invert.RangeIndex;
import io.minipinot.store.ColumnMetadata;
import io.minipinot.store.IndexType;
import io.minipinot.store.SegmentDirectory;
import java.nio.ByteBuffer;

/**
 * {@link DataSource} backed by the memory-mapped single-file segment ({@link SegmentDirectory}).
 * Readers are constructed once from the mapped buffers; nothing is copied onto the heap. Mirrors
 * Pinot's {@code ImmutableDataSource} + {@code PhysicalColumnIndexContainer} (which build the
 * per-column readers from the mmap'd {@code columns.psf}).
 */
public final class ImmutableDataSource implements DataSource {
  private final ColumnMetadata _metadata;
  private final SortedDictionary _dictionary;
  private final ForwardIndexReader _forwardIndex;
  private final BitmapInvertedIndex.Reader _invertedIndex;
  private final RangeIndex.Reader _rangeIndex;
  private final BloomFilter.Reader _bloomFilter;

  public ImmutableDataSource(SegmentDirectory dir, ColumnMetadata metadata,
      ForwardIndexEncoding forwardIndexEncoding) {
    _metadata = metadata;
    String column = metadata.getName();

    _dictionary = new SortedDictionary(metadata.getDataType(), metadata.getCardinality(),
        metadata.getDictionaryStride(), dir.getBuffer(column, IndexType.DICTIONARY), 0);

    _forwardIndex = buildForwardIndex(dir, metadata, forwardIndexEncoding);

    ByteBuffer invBuf = dir.getBuffer(column, IndexType.INVERTED_INDEX);
    _invertedIndex = invBuf == null ? null : new BitmapInvertedIndex.Reader(invBuf);

    ByteBuffer rangeBuf = dir.getBuffer(column, IndexType.RANGE_INDEX);
    _rangeIndex = rangeBuf == null ? null : new RangeIndex.Reader(rangeBuf);

    ByteBuffer bloomBuf = dir.getBuffer(column, IndexType.BLOOM_FILTER);
    _bloomFilter = bloomBuf == null ? null : new BloomFilter.Reader(bloomBuf);
  }

  private static ForwardIndexReader buildForwardIndex(SegmentDirectory dir, ColumnMetadata metadata,
      ForwardIndexEncoding encoding) {
    String column = metadata.getName();
    ByteBuffer buffer = dir.getBuffer(column, IndexType.FORWARD_INDEX);
    int totalDocs = metadata.getTotalDocs();
    if (metadata.isSorted()) {
      return new SortedForwardIndex.Reader(buffer, 0, metadata.getCardinality(), totalDocs);
    }
    if (encoding == ForwardIndexEncoding.LUCENE_DIRECT) {
      return new LuceneFixedBitForwardIndex.Reader(buffer, totalDocs, metadata.getNumBitsPerElement());
    }
    return new FixedBitForwardIndex.Reader(buffer, 0, totalDocs, metadata.getNumBitsPerElement());
  }

  @Override
  public ColumnMetadata getDataSourceMetadata() {
    return _metadata;
  }

  @Override
  public SortedDictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public ForwardIndexReader getForwardIndex() {
    return _forwardIndex;
  }

  @Override
  public BitmapInvertedIndex.Reader getInvertedIndex() {
    return _invertedIndex;
  }

  @Override
  public RangeIndex.Reader getRangeIndex() {
    return _rangeIndex;
  }

  @Override
  public BloomFilter.Reader getBloomFilter() {
    return _bloomFilter;
  }
}
