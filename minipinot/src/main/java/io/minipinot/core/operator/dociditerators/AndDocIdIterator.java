package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;

/**
 * Intersection ({@code AND}) of child iterators using the leap-frog algorithm: repeatedly take the
 * maximum current document across children and {@link BlockDocIdIterator#advance} the laggards up to
 * it; when all children agree on a document it is a match. This is why {@code advance} exists — it
 * lets a bitmap or sorted range skip ahead cheaply instead of stepping one document at a time.
 * Mirrors Pinot's {@code AndDocIdIterator}.
 */
public final class AndDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator[] _iterators;
  private final int[] _currentDocs;
  private int _nextCandidate;
  private boolean _exhausted;

  public AndDocIdIterator(BlockDocIdIterator[] iterators) {
    _iterators = iterators;
    _currentDocs = new int[iterators.length];
    for (int i = 0; i < iterators.length; i++) {
      _currentDocs[i] = iterators[i].next();
      if (_currentDocs[i] == Constants.EOF) {
        _exhausted = true;
      }
    }
  }

  @Override
  public int next() {
    if (_exhausted) {
      return Constants.EOF;
    }
    int candidate = _nextCandidate;
    while (true) {
      int max = candidate;
      for (int i = 0; i < _iterators.length; i++) {
        if (_currentDocs[i] < candidate) {
          _currentDocs[i] = _iterators[i].advance(candidate);
          if (_currentDocs[i] == Constants.EOF) {
            _exhausted = true;
            return Constants.EOF;
          }
        }
        if (_currentDocs[i] > max) {
          max = _currentDocs[i];
        }
      }
      if (max == candidate) {
        _nextCandidate = candidate + 1;
        return candidate;
      }
      candidate = max;
    }
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId > _nextCandidate) {
      _nextCandidate = targetDocId;
    }
    return next();
  }
}
