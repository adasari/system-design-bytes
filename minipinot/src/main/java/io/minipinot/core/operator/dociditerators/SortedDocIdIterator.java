package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;
import java.util.List;

/**
 * Iterates a sorted list of inclusive document-id ranges {@code [start, end]} with no materialization.
 * This is the representation used by a sorted-column filter, where each dictId (or dictId range) maps
 * to a contiguous run of documents.
 */
public final class SortedDocIdIterator implements BlockDocIdIterator {
  private final List<int[]> _ranges;
  private int _rangeIndex;
  private int _nextDoc;
  private boolean _started;

  public SortedDocIdIterator(List<int[]> ranges) {
    _ranges = ranges;
  }

  @Override
  public int next() {
    if (!_started) {
      _started = true;
      if (_ranges.isEmpty()) {
        _rangeIndex = 0;
        return Constants.EOF;
      }
      _nextDoc = _ranges.get(0)[0];
    }
    while (_rangeIndex < _ranges.size()) {
      int[] range = _ranges.get(_rangeIndex);
      if (_nextDoc <= range[1]) {
        return _nextDoc++;
      }
      _rangeIndex++;
      if (_rangeIndex < _ranges.size()) {
        _nextDoc = _ranges.get(_rangeIndex)[0];
      }
    }
    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _started = true;
    while (_rangeIndex < _ranges.size() && _ranges.get(_rangeIndex)[1] < targetDocId) {
      _rangeIndex++;
    }
    if (_rangeIndex >= _ranges.size()) {
      return Constants.EOF;
    }
    _nextDoc = Math.max(targetDocId, _ranges.get(_rangeIndex)[0]);
    return next();
  }
}
