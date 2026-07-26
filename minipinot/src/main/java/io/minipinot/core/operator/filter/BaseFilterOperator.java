package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;

/**
 * Base class for filter operators. A filter operator evaluates a piece of the {@code WHERE} tree and
 * exposes the matching documents of one segment as a lazily-consumable {@link BlockDocIdSet} — never
 * a fully materialized collection, so a sorted range, a lazy scan and a bitmap all compose uniformly.
 *
 * <p>Mirrors Pinot's {@code BaseFilterOperator}, whose block exposes a {@code BlockDocIdSet}.
 */
public abstract class BaseFilterOperator {
  protected final int _numDocs;

  protected BaseFilterOperator(int numDocs) {
    _numDocs = numDocs;
  }

  /** The set of documents this operator matches, as a lazily-consumable doc-id set. */
  public abstract BlockDocIdSet getDocIds();

  /**
   * Whether this operator is guaranteed to match every document in the segment. Used by the plan
   * maker to switch aggregations to the metadata-only path (no scan). Mirrors Pinot's
   * {@code BaseFilterOperator#isResultMatchingAll()}.
   */
  public boolean isResultMatchingAll() {
    return false;
  }
}
