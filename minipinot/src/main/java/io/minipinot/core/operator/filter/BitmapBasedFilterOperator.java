package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BitmapDocIdSet;
import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.invert.BitmapInvertedIndex;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Resolves a predicate by unioning the inverted-index bitmaps of the matching dictIds and exposing
 * the result as a bitmap doc-id set. This is the fast path for {@code EQ}/{@code IN} on a
 * dictionary-encoded column with an inverted index. Mirrors Pinot's {@code BitmapBasedFilterOperator}.
 *
 * <p>It can also wrap a pre-computed doc-id bitmap directly (e.g. a segment's queryable-docs
 * snapshot), which is exactly how Pinot uses {@code new BitmapBasedFilterOperator(snapshot, false,
 * numDocs)} to intersect a query with the set of currently valid documents.
 */
public final class BitmapBasedFilterOperator extends BaseFilterOperator {
  private final BitmapInvertedIndex.Reader _invertedIndex;
  private final int[] _dictIds;
  private final ImmutableRoaringBitmap _docIds;

  public BitmapBasedFilterOperator(BitmapInvertedIndex.Reader invertedIndex, int[] dictIds,
      int numDocs) {
    super(numDocs);
    _invertedIndex = invertedIndex;
    _dictIds = dictIds;
    _docIds = null;
  }

  /** Wrap a pre-computed doc-id bitmap directly (no dictId union). */
  public BitmapBasedFilterOperator(ImmutableRoaringBitmap docIds, int numDocs) {
    super(numDocs);
    _invertedIndex = null;
    _dictIds = null;
    _docIds = docIds;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    if (_docIds != null) {
      return new BitmapDocIdSet(_docIds);
    }
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (int dictId : _dictIds) {
      if (dictId >= 0) {
        result.or(_invertedIndex.getDocIds(dictId));
      }
    }
    return new BitmapDocIdSet(result);
  }
}
