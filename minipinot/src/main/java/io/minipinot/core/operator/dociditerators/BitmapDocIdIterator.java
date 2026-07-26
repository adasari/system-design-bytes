package io.minipinot.core.operator.dociditerators;

import io.minipinot.core.common.Constants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * Iterates the set bits of a (materialized) RoaringBitmap, using its {@link PeekableIntIterator} so
 * {@link #advance(int)} can skip whole containers. Backs the inverted-index and range-index access
 * paths, which are naturally bitmap-shaped. Mirrors Pinot's {@code BitmapDocIdIterator}.
 */
public final class BitmapDocIdIterator implements BlockDocIdIterator {
  private final PeekableIntIterator _iterator;

  public BitmapDocIdIterator(ImmutableRoaringBitmap bitmap) {
    _iterator = bitmap.getIntIterator();
  }

  @Override
  public int next() {
    return _iterator.hasNext() ? _iterator.next() : Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    _iterator.advanceIfNeeded(targetDocId);
    return next();
  }
}
