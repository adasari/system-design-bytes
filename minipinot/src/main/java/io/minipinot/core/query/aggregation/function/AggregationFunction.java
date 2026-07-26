package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.segment.DataSource;

/**
 * An aggregation function using an accumulator model that is safe to merge across segments. Mirrors
 * the contract of Pinot's {@code AggregationFunction} (accumulate over documents, merge partial
 * results, then extract the final value), reduced to the essentials.
 *
 * <p>The engine calls {@link #createAccumulator()} once per group, folds each document's value in via
 * {@link #aggregate}, {@link #merge}s the per-segment accumulators at the broker, and finally calls
 * {@link #extractFinalResult}. Keeping intermediate and final results distinct (e.g. {@code AVG}
 * carries a sum/count pair) is exactly what makes multi-segment reduction correct.
 *
 * <p>Some functions can also be answered directly from segment/column metadata when the whole segment
 * is matched (see {@link #isMetadataBased()}), avoiding any per-document work — this mirrors Pinot's
 * {@code NonScanBasedAggregationOperator}.
 */
public interface AggregationFunction {

  /** The result column name, e.g. {@code count(*)} or {@code sum(clicks)}. */
  String getResultName();

  /** The argument expression (a column identifier, or {@code *} for {@code COUNT(*)}). */
  ExpressionContext getInputExpression();

  /** Whether this function needs the column value (false for {@code COUNT(*)}, which only counts). */
  boolean needsInputValue();

  Object createAccumulator();

  /** Fold one document's column value into the accumulator; {@code value} is null when not needed. */
  Object aggregate(Object accumulator, Object value);

  /** Merge two per-segment accumulators (broker-side reduction). */
  Object merge(Object a, Object b);

  /** Produce the user-visible final value from an accumulator. */
  Object extractFinalResult(Object accumulator);

  /**
   * Whether this function can be answered from segment/column metadata alone when the entire segment
   * is matched. In Pinot this is the {@code METADATA_BASED_FUNCTIONS} set: {@code COUNT} (from
   * {@code totalDocs}), {@code MIN}/{@code MAX} (from the sorted dictionary's first/last value). Note
   * {@code SUM}/{@code AVG} are <em>not</em> metadata-based — no precomputed sum is stored.
   */
  default boolean isMetadataBased() {
    return false;
  }

  /**
   * Compute the intermediate accumulator directly from metadata (only valid when
   * {@link #isMetadataBased()}). The returned accumulator has the same shape as the scan path's, so
   * the broker merges it identically. {@code dataSource} is null for {@code COUNT(*)}.
   */
  default Object aggregateFromMetadata(DataSource dataSource, int numTotalDocs) {
    throw new UnsupportedOperationException("Not a metadata-based aggregation: " + getResultName());
  }
}

