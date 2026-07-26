package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.segment.DataSource;

/** {@code MIN} of a numeric column; the accumulator is a running {@link Double}. Answered from the
 * dictionary minimum with no scan when the whole segment is matched. Mirrors Pinot's
 * {@code MinAggregationFunction}. */
public final class MinAggregationFunction extends BaseAggregationFunction {

  public MinAggregationFunction(ExpressionContext inputExpression) {
    super("min", inputExpression);
  }

  @Override
  public boolean needsInputValue() {
    return true;
  }

  @Override
  public Object createAccumulator() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public Object aggregate(Object accumulator, Object value) {
    return Math.min((Double) accumulator, toDouble(value));
  }

  @Override
  public Object merge(Object a, Object b) {
    return Math.min((Double) a, (Double) b);
  }

  @Override
  public Object extractFinalResult(Object accumulator) {
    return accumulator;
  }

  @Override
  public boolean isMetadataBased() {
    return true;
  }

  @Override
  public Object aggregateFromMetadata(DataSource dataSource, int numTotalDocs) {
    return dictionaryMin(dataSource);
  }
}
