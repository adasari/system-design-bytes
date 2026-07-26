package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;

/**
 * Complement ({@code NOT}) of a child iterator over {@code [0, numDocs)}. It walks candidate
 * documents in ascending order, emitting those the child does not match; because both streams are
 * ascending this needs only a single synchronized pass and no materialization. Mirrors Pinot's
 * {@code NotDocIdIterator}.
 */
public final class NotDocIdIterator implements BlockDocIdIterator {
  private final BlockDocIdIterator _child;
  private final int _numDocs;
  private int _candidate;
  private int _childNext;

  public NotDocIdIterator(BlockDocIdIterator child, int numDocs) {
    _child = child;
    _numDocs = numDocs;
    _childNext = child.next();
  }

  @Override
  public int next() {
    while (_candidate < _numDocs) {
      if (_childNext != Constants.EOF && _childNext <= _candidate) {
        if (_childNext == _candidate) {
          _candidate++;
        }
        _childNext = _child.next();
        continue;
      }
      return _candidate++;
    }
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId > _candidate) {
      _candidate = targetDocId;
      if (_childNext != Constants.EOF && _childNext < _candidate) {
        _childNext = _child.advance(_candidate);
      }
    }
    return next();
  }
}
