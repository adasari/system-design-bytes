package io.minipinot.core.operator.projection;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.segment.DataSource;

/**
 * Reads a column's value for a document id by chaining the two on-disk structures that make up a
 * dictionary-encoded column: {@code forwardIndex.getDictId(docId)} then {@code dictionary.get(dictId)}.
 * This is exactly the projection step Pinot performs (via {@code DataFetcher} /
 * {@code ForwardIndexReader} + {@code Dictionary}); MiniPinot collapses it into one tiny helper so the
 * query operators stay readable.
 */
public final class ColumnValueReader {
  private final SortedDictionary _dictionary;
  private final ForwardIndexReader _forwardIndex;

  public ColumnValueReader(DataSource dataSource) {
    _dictionary = dataSource.getDictionary();
    _forwardIndex = dataSource.getForwardIndex();
  }

  /** Materialize the column value at {@code docId}: {@code dictionary.get(forward.getDictId(docId))}. */
  public Object getValue(int docId) {
    return _dictionary.get(_forwardIndex.getDictId(docId));
  }
}
