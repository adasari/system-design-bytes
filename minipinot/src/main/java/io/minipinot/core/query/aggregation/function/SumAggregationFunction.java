package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;

/** {@code SUM} of a numeric column; the accumulator is a running {@link Double}. Mirrors Pinot's
 * {@code SumAggregationFunction}. */
public final class SumAggregationFunction extends BaseAggregationFunction {

  public SumAggregationFunction(ExpressionContext inputExpression) {
    super("sum", inputExpression);
  }

  @Override
  public boolean needsInputValue() {
    return true;
  }

  @Override
  public Object createAccumulator() {
    return 0.0;
  }

  @Override
  public Object aggregate(Object accumulator, Object value) {
    return (Double) accumulator + toDouble(value);
  }

  @Override
  public Object merge(Object a, Object b) {
    return (Double) a + (Double) b;
  }

  @Override
  public Object extractFinalResult(Object accumulator) {
    return accumulator;
  }
}
