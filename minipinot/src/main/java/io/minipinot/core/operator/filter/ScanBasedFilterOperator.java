package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.core.operator.docidsets.ScanBasedDocIdSet;
import io.minipinot.forward.ForwardIndexReader;
import java.util.function.IntPredicate;

/**
 * The universal fallback: exposes a lazy scan that tests each document's dictId against a matcher.
 * Used when no suitable index exists for the predicate. Because every predicate on a
 * dictionary-encoded column reduces to a test on order-preserving dictIds (equality, membership, or a
 * {@code [low, high]} range), the matcher is a simple {@link IntPredicate} over dictIds. Mirrors
 * Pinot's {@code ScanBasedFilterOperator}; it also doubles as the correctness oracle for indexed paths.
 */
public final class ScanBasedFilterOperator extends BaseFilterOperator {
  private final ForwardIndexReader _forwardIndex;
  private final IntPredicate _dictIdMatcher;

  public ScanBasedFilterOperator(ForwardIndexReader forwardIndex, IntPredicate dictIdMatcher,
      int numDocs) {
    super(numDocs);
    _forwardIndex = forwardIndex;
    _dictIdMatcher = dictIdMatcher;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    return new ScanBasedDocIdSet(_forwardIndex, _dictIdMatcher, _numDocs);
  }
}
