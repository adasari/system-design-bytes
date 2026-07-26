package io.minipinot.core.request;

/**
 * One key of an {@code ORDER BY} clause: the expression to sort on plus its direction. Mirrors
 * Pinot's {@code org.apache.pinot.common.request.context.OrderByExpressionContext}.
 */
public final class OrderByExpressionContext {
  private final ExpressionContext _expression;
  private final boolean _asc;

  public OrderByExpressionContext(ExpressionContext expression, boolean asc) {
    _expression = expression;
    _asc = asc;
  }

  public ExpressionContext getExpression() {
    return _expression;
  }

  public boolean isAsc() {
    return _asc;
  }

  @Override
  public String toString() {
    return _expression + (_asc ? " ASC" : " DESC");
  }
}
