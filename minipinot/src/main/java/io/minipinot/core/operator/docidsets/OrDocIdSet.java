package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.dociditerators.OrDocIdIterator;
import java.util.List;

/** Union of child doc-id sets ({@code OR}). */
public final class OrDocIdSet implements BlockDocIdSet {
  private final List<BlockDocIdSet> _children;

  public OrDocIdSet(List<BlockDocIdSet> children) {
    _children = children;
  }

  @Override
  public BlockDocIdIterator iterator() {
    BlockDocIdIterator[] iterators = new BlockDocIdIterator[_children.size()];
    for (int i = 0; i < iterators.length; i++) {
      iterators[i] = _children.get(i).iterator();
    }
    return new OrDocIdIterator(iterators);
  }
}
