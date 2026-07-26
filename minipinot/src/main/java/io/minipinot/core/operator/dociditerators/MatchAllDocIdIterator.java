package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;

/** Iterates every document id in {@code [0, numDocs)}. Backs a query with no {@code WHERE}. */
public final class MatchAllDocIdIterator implements BlockDocIdIterator {
  private final int _numDocs;
  private int _next;

  public MatchAllDocIdIterator(int numDocs) {
    _numDocs = numDocs;
  }

  @Override
  public int next() {
    return _next < _numDocs ? _next++ : Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _next = Math.max(_next, targetDocId);
    return next();
  }
}
