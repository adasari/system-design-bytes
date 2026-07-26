package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;
import io.minipinot.forward.ForwardIndexReader;
import java.util.function.IntPredicate;

/**
 * Lazily scans documents, decoding each dictId from the forward index and testing it against a
 * matcher. Nothing is materialized: documents are produced on demand as the consumer pulls. Mirrors
 * the spirit of Pinot's {@code ScanBasedDocIdIterator}.
 */
public final class ScanBasedDocIdIterator implements BlockDocIdIterator {
  private final ForwardIndexReader _forwardIndex;
  private final IntPredicate _dictIdMatcher;
  private final int _numDocs;
  private int _next;

  public ScanBasedDocIdIterator(ForwardIndexReader forwardIndex, IntPredicate dictIdMatcher,
      int numDocs) {
    _forwardIndex = forwardIndex;
    _dictIdMatcher = dictIdMatcher;
    _numDocs = numDocs;
  }

  @Override
  public int next() {
    while (_next < _numDocs) {
      int docId = _next++;
      if (_dictIdMatcher.test(_forwardIndex.getDictId(docId))) {
        return docId;
      }
    }
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _next = Math.max(_next, targetDocId);
    return next();
  }
}
