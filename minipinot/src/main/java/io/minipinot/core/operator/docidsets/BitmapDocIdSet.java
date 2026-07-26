package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BitmapDocIdIterator;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/** A materialized RoaringBitmap of documents (inverted-index / range-index access path). */
public final class BitmapDocIdSet implements BlockDocIdSet {
  private final ImmutableRoaringBitmap _bitmap;

  public BitmapDocIdSet(ImmutableRoaringBitmap bitmap) {
    _bitmap = bitmap;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new BitmapDocIdIterator(_bitmap);
  }
}
