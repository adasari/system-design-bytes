package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;
import java.util.PriorityQueue;

/**
 * Union ({@code OR}) of child iterators via a min-heap keyed on each child's current document. On
 * every {@link #next()} the smallest document is emitted once and every child positioned on it is
 * advanced, which naturally de-duplicates documents matched by more than one child. Mirrors Pinot's
 * {@code OrDocIdIterator}.
 */
public final class OrDocIdIterator implements BlockDocIdIterator {
  private static final class Entry {
    int _doc;
    final BlockDocIdIterator _iterator;

    Entry(int doc, BlockDocIdIterator iterator) {
      _doc = doc;
      _iterator = iterator;
    }
  }

  private final PriorityQueue<Entry> _heap;

  public OrDocIdIterator(BlockDocIdIterator[] iterators) {
    _heap = new PriorityQueue<>(Math.max(1, iterators.length), (a, b) -> Integer.compare(a._doc, b._doc));
    for (BlockDocIdIterator iterator : iterators) {
      int doc = iterator.next();
      if (doc != Constants.EOF) {
        _heap.add(new Entry(doc, iterator));
      }
    }
  }

  @Override
  public int next() {
    if (_heap.isEmpty()) {
      return Constants.EOF;
    }
    int min = _heap.peek()._doc;
    while (!_heap.isEmpty() && _heap.peek()._doc == min) {
      Entry entry = _heap.poll();
      int nextDoc = entry._iterator.next();
      if (nextDoc != Constants.EOF) {
        entry._doc = nextDoc;
        _heap.add(entry);
      }
    }
    return min;
  }

  @Override
  public int advance(int targetDocId) {
    int doc;
    do {
      doc = next();
    } while (doc != Constants.EOF && doc < targetDocId);
    return doc;
  }
}
