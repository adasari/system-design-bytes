package io.minipinot.core.operator.docidsets;

import io.minipinot.core.common.Constants;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;

/** The empty match (no documents). */
public final class EmptyDocIdSet implements BlockDocIdSet {
  private static final EmptyDocIdSet INSTANCE = new EmptyDocIdSet();

  private EmptyDocIdSet() {
  }

  public static EmptyDocIdSet getInstance() {
    return INSTANCE;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new BlockDocIdIterator() {
      @Override
      public int next() {
        return Constants.EOF;
      }

      @Override
      public int advance(int targetDocId) {
        return Constants.EOF;
      }
    };
  }
}
