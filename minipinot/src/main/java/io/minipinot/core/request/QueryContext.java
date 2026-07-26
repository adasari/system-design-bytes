package io.minipinot.core.request;

import java.util.List;

/**
 * The fully resolved, execution-ready representation of a query — the output of the parser and the
 * input to the plan maker. Mirrors Pinot's {@code org.apache.pinot.core.query.request.context.QueryContext},
 * which is what both the server (per-segment execution) and the broker (result reduce) operate on.
 *
 * <p>Holds the select list, optional {@code WHERE} filter tree, group-by keys, order-by keys and the
 * {@code LIMIT}/{@code OFFSET}. Convenience predicates ({@link #isAggregationQuery()},
 * {@link #isGroupByQuery()}) let the plan maker choose the operator tree, exactly as Pinot does.
 */
public final class QueryContext {
  /** Pinot's default when no LIMIT is specified. */
  public static final int DEFAULT_LIMIT = 10;

  private final List<ExpressionContext> _selectExpressions;
  private final FilterContext _filter;
  private final List<ExpressionContext> _groupByExpressions;
  private final FilterContext _havingFilter;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final int _limit;
  private final int _offset;

  private QueryContext(Builder builder) {
    _selectExpressions = builder._selectExpressions;
    _filter = builder._filter;
    _groupByExpressions = builder._groupByExpressions;
    _havingFilter = builder._havingFilter;
    _orderByExpressions = builder._orderByExpressions;
    _limit = builder._limit;
    _offset = builder._offset;
  }

  public List<ExpressionContext> getSelectExpressions() {
    return _selectExpressions;
  }

  /** {@code null} when the query has no {@code WHERE} clause (match-all). */
  public FilterContext getFilter() {
    return _filter;
  }

  /** {@code null} when the query has no {@code GROUP BY}. */
  public List<ExpressionContext> getGroupByExpressions() {
    return _groupByExpressions;
  }

  /** {@code null} when the query has no {@code HAVING}. Applies to grouped aggregation results. */
  public FilterContext getHavingFilter() {
    return _havingFilter;
  }

  /** {@code null} when the query has no {@code ORDER BY}. */
  public List<OrderByExpressionContext> getOrderByExpressions() {
    return _orderByExpressions;
  }

  public int getLimit() {
    return _limit;
  }

  public int getOffset() {
    return _offset;
  }

  public boolean isGroupByQuery() {
    return _groupByExpressions != null && !_groupByExpressions.isEmpty();
  }

  /**
   * True if any select expression is an aggregation function (or the query groups by, which always
   * implies aggregation in this engine). Mirrors how Pinot distinguishes selection from aggregation
   * queries when picking the operator tree.
   */
  public boolean isAggregationQuery() {
    if (isGroupByQuery()) {
      return true;
    }
    for (ExpressionContext expression : _selectExpressions) {
      if (expression.getType() == ExpressionContext.Type.FUNCTION
          && expression.getFunction().getType() == FunctionContext.Type.AGGREGATION) {
        return true;
      }
    }
    return false;
  }

  public static final class Builder {
    private List<ExpressionContext> _selectExpressions;
    private FilterContext _filter;
    private List<ExpressionContext> _groupByExpressions;
    private FilterContext _havingFilter;
    private List<OrderByExpressionContext> _orderByExpressions;
    private int _limit = DEFAULT_LIMIT;
    private int _offset = 0;

    public Builder setSelectExpressions(List<ExpressionContext> selectExpressions) {
      _selectExpressions = selectExpressions;
      return this;
    }

    public Builder setFilter(FilterContext filter) {
      _filter = filter;
      return this;
    }

    public Builder setGroupByExpressions(List<ExpressionContext> groupByExpressions) {
      _groupByExpressions = groupByExpressions;
      return this;
    }

    public Builder setHavingFilter(FilterContext havingFilter) {
      _havingFilter = havingFilter;
      return this;
    }

    public Builder setOrderByExpressions(List<OrderByExpressionContext> orderByExpressions) {
      _orderByExpressions = orderByExpressions;
      return this;
    }

    public Builder setLimit(int limit) {
      _limit = limit;
      return this;
    }

    public Builder setOffset(int offset) {
      _offset = offset;
      return this;
    }

    public QueryContext build() {
      return new QueryContext(this);
    }
  }
}
