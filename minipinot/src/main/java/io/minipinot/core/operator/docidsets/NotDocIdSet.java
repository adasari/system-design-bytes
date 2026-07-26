package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.dociditerators.NotDocIdIterator;

/** Complement of a child doc-id set over {@code [0, numDocs)} ({@code NOT}). */
public final class NotDocIdSet implements BlockDocIdSet {
  private final BlockDocIdSet _child;
  private final int _numDocs;

  public NotDocIdSet(BlockDocIdSet child, int numDocs) {
    _child = child;
    _numDocs = numDocs;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new NotDocIdIterator(_child.iterator(), _numDocs);
  }
}
