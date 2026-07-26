package io.minipinot.core.operator.docidsets;

import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.dociditerators.SortedDocIdIterator;
import java.util.List;

/** A sorted list of inclusive document-id ranges {@code [start, end]} (sorted-column access path). */
public final class SortedDocIdSet implements BlockDocIdSet {
  private final List<int[]> _ranges;

  public SortedDocIdSet(List<int[]> ranges) {
    _ranges = ranges;
  }

  @Override
  public BlockDocIdIterator iterator() {
    return new SortedDocIdIterator(_ranges);
  }
}
