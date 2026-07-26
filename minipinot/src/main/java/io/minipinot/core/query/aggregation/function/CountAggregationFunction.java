package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.segment.DataSource;

/**
 * {@code COUNT} — counts matched documents. Because MiniPinot has no nulls, {@code COUNT(*)} and
 * {@code COUNT(col)} behave identically and neither needs to read the column value. The accumulator
 * is a running {@link Long}. When the whole segment is matched it is answered from
 * {@code totalDocs} with no scan. Mirrors Pinot's {@code CountAggregationFunction}.
 */
public final class CountAggregationFunction extends BaseAggregationFunction {

  public CountAggregationFunction(ExpressionContext inputExpression) {
    super("count", inputExpression);
  }

  @Override
  public boolean needsInputValue() {
    return false;
  }

  @Override
  public Object createAccumulator() {
    return 0L;
  }

  @Override
  public Object aggregate(Object accumulator, Object value) {
    return (Long) accumulator + 1L;
  }

  @Override
  public Object merge(Object a, Object b) {
    return (Long) a + (Long) b;
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
    return (long) numTotalDocs;
  }
}
