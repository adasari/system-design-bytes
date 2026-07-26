package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.dociditerators.MatchAllDocIdIterator;

/** All documents in {@code [0, numDocs)}. */
public final class MatchAllDocIdSet implements BlockDocIdSet {
  private final int _numDocs;

  public MatchAllDocIdSet(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new MatchAllDocIdIterator(_numDocs);
  }
}
