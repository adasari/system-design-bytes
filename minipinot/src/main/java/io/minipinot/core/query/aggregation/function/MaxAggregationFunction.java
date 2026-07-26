package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.segment.DataSource;

/** {@code MAX} of a numeric column; the accumulator is a running {@link Double}. Answered from the
 * dictionary maximum with no scan when the whole segment is matched. Mirrors Pinot's
 * {@code MaxAggregationFunction}. */
public final class MaxAggregationFunction extends BaseAggregationFunction {

  public MaxAggregationFunction(ExpressionContext inputExpression) {
    super("max", inputExpression);
  }

  @Override
  public boolean needsInputValue() {
    return true;
  }

  @Override
  public Object createAccumulator() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public Object aggregate(Object accumulator, Object value) {
    return Math.max((Double) accumulator, toDouble(value));
  }

  @Override
  public Object merge(Object a, Object b) {
    return Math.max((Double) a, (Double) b);
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
    return dictionaryMax(dataSource);
  }
}
