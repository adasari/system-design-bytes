package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;

/**
 * {@code AVG} of a numeric column. The accumulator is an {@link AvgPair} (sum + count) so per-segment
 * partial averages merge correctly; the final result is {@code sum / count}. Mirrors Pinot's
 * {@code AvgAggregationFunction}.
 */
public final class AvgAggregationFunction extends BaseAggregationFunction {

  public AvgAggregationFunction(ExpressionContext inputExpression) {
    super("avg", inputExpression);
  }

  @Override
  public boolean needsInputValue() {
    return true;
  }

  @Override
  public Object createAccumulator() {
    return new AvgPair(0.0, 0L);
  }

  @Override
  public Object aggregate(Object accumulator, Object value) {
    ((AvgPair) accumulator).apply(toDouble(value));
    return accumulator;
  }

  @Override
  public Object merge(Object a, Object b) {
    return ((AvgPair) a).merge((AvgPair) b);
  }

  @Override
  public Object extractFinalResult(Object accumulator) {
    return ((AvgPair) accumulator).toAverage();
  }
}
