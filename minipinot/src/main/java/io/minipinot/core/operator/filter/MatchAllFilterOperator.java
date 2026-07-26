package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.core.operator.docidsets.MatchAllDocIdSet;

/** Matches every document (a query with no {@code WHERE}). Mirrors Pinot's
 * {@code MatchAllFilterOperator}. */
public final class MatchAllFilterOperator extends BaseFilterOperator {

  public MatchAllFilterOperator(int numDocs) {
    super(numDocs);
  }

  @Override
  public BlockDocIdSet getDocIds() {
    return new MatchAllDocIdSet(_numDocs);
  }

  @Override
  public boolean isResultMatchingAll() {
    return true;
  }
}
