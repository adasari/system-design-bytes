package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.core.operator.docidsets.EmptyDocIdSet;

/** Matches no documents (absent equality value or bloom-filter miss). Mirrors Pinot's
 * {@code EmptyFilterOperator}. */
public final class EmptyFilterOperator extends BaseFilterOperator {

  public EmptyFilterOperator(int numDocs) {
    super(numDocs);
  }

  @Override
  public BlockDocIdSet getDocIds() {
    return EmptyDocIdSet.getInstance();
  }
}
