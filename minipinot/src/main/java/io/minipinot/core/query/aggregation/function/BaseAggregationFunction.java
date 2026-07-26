package io.minipinot.core.query.aggregation.function;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.segment.DataSource;

/** Common state for aggregation functions: the argument expression and the result column name. */
abstract class BaseAggregationFunction implements AggregationFunction {
  protected final ExpressionContext _inputExpression;
  private final String _resultName;

  BaseAggregationFunction(String functionName, ExpressionContext inputExpression) {
    _inputExpression = inputExpression;
    _resultName = functionName + "(" + inputExpression + ")";
  }

  @Override
  public String getResultName() {
    return _resultName;
  }

  @Override
  public ExpressionContext getInputExpression() {
    return _inputExpression;
  }

  protected static double toDouble(Object value) {
    return ((Number) value).doubleValue();
  }

  /** The column minimum straight from the sorted dictionary's first value (mirrors Pinot's
   * {@code dictionary.getMinVal()}). */
  protected static double dictionaryMin(DataSource dataSource) {
    return toDouble(dataSource.getDictionary().get(0));
  }

  /** The column maximum straight from the sorted dictionary's last value (mirrors Pinot's
   * {@code dictionary.getMaxVal()}). */
  protected static double dictionaryMax(DataSource dataSource) {
    int cardinality = dataSource.getDataSourceMetadata().getCardinality();
    return toDouble(dataSource.getDictionary().get(cardinality - 1));
  }
}
