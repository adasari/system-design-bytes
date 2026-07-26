package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.AndDocIdIterator;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import java.util.List;

/** Intersection of child doc-id sets ({@code AND}). */
public final class AndDocIdSet implements BlockDocIdSet {
  private final List<BlockDocIdSet> _children;

  public AndDocIdSet(List<BlockDocIdSet> children) {
    _children = children;
  }

  @Override
  public BlockDocIdIterator iterator() {
    BlockDocIdIterator[] iterators = new BlockDocIdIterator[_children.size()];
    for (int i = 0; i < iterators.length; i++) {
      iterators[i] = _children.get(i).iterator();
    }
    return new AndDocIdIterator(iterators);
  }
}
