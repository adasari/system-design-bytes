package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BitmapDocIdSet;
import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.invert.RangeIndex;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Resolves a range predicate using the (bucketed) range index. Fully-covered buckets are taken as-is;
 * boundary buckets are re-checked against the forward index so only documents whose dictId truly lies
 * in {@code [lowDictId, highDictId]} are kept. The result is exposed as a bitmap doc-id set. Mirrors
 * Pinot's {@code RangeIndexBasedFilterOperator}, which likewise re-evaluates partially-matching docs.
 */
public final class RangeIndexBasedFilterOperator extends BaseFilterOperator {
  private final RangeIndex.Reader _rangeIndex;
  private final ForwardIndexReader _forwardIndex;
  private final int _lowDictId;
  private final int _highDictId;

  public RangeIndexBasedFilterOperator(RangeIndex.Reader rangeIndex, ForwardIndexReader forwardIndex,
      int lowDictId, int highDictId, int numDocs) {
    super(numDocs);
    _rangeIndex = rangeIndex;
    _forwardIndex = forwardIndex;
    _lowDictId = lowDictId;
    _highDictId = highDictId;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    if (_lowDictId > _highDictId) {
      return new BitmapDocIdSet(new MutableRoaringBitmap());
    }
    RangeIndex.RangeMatch match = _rangeIndex.query(_lowDictId, _highDictId);
    MutableRoaringBitmap result = match._fullyMatching.clone();
    IntIterator iterator = match._partiallyMatching.getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      int dictId = _forwardIndex.getDictId(docId);
      if (dictId >= _lowDictId && dictId <= _highDictId) {
        result.add(docId);
      }
    }
    return new BitmapDocIdSet(result);
  }
}
