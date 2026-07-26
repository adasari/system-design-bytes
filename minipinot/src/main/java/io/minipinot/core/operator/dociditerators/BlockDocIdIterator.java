package io.minipinot.core.operator.dociditerators;

/**
 * A forward-only cursor over matching document ids, produced by a {@code BlockDocIdSet}. Document
 * ids are returned in ascending order; {@link io.minipinot.core.common.Constants#EOF} signals the
 * end. This is the core of Pinot's lazy filtering model — it lets a sorted range, a lazy scan and a
 * materialized bitmap all be consumed uniformly without forcing any of them to materialize.
 *
 * <p>Mirrors Pinot's {@code org.apache.pinot.core.operator.dociditerators.BlockDocIdIterator}.
 */
public interface BlockDocIdIterator {

  /** Return the next matching document id, or {@code EOF} if exhausted. */
  int next();

  /**
   * Return the first matching document id greater than or equal to {@code targetDocId}, or
   * {@code EOF} if there is none. Enables efficient leap-frog intersection in {@code AND}.
   */
  int advance(int targetDocId);
}
