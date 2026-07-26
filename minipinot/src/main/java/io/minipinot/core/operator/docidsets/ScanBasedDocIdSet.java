package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import io.minipinot.forward.ForwardIndexReader;
import java.util.function.IntPredicate;

/** A lazy full scan: documents are produced on demand by testing each dictId against a matcher. */
public final class ScanBasedDocIdSet implements BlockDocIdSet {
  private final ForwardIndexReader _forwardIndex;
  private final IntPredicate _dictIdMatcher;
  private final int _numDocs;

  public ScanBasedDocIdSet(ForwardIndexReader forwardIndex, IntPredicate dictIdMatcher, int numDocs) {
    _forwardIndex = forwardIndex;
    _dictIdMatcher = dictIdMatcher;
    _numDocs = numDocs;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new ScanBasedDocIdIterator(_forwardIndex, _dictIdMatcher, _numDocs);
  }
}
