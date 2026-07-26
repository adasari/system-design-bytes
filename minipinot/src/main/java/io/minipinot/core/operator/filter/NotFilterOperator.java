package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.core.operator.docidsets.NotDocIdSet;

/** Complements its child's matching documents within {@code [0, numDocs)} ({@code NOT}). Mirrors
 * Pinot's {@code NotFilterOperator}. */
public final class NotFilterOperator extends BaseFilterOperator {
  private final BaseFilterOperator _child;

  public NotFilterOperator(BaseFilterOperator child, int numDocs) {
    super(numDocs);
    _child = child;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    return new NotDocIdSet(_child.getDocIds(), _numDocs);
  }
}
