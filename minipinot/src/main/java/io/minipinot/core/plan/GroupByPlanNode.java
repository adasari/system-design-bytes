package io.minipinot.core.plan;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.query.GroupByOperator;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.aggregation.function.AggregationFunctionFactory;
import io.minipinot.core.request.ExpressionContext;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.IndexSegment;
import io.minipinot.segment.SegmentContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans a grouped aggregation over one segment. It builds the segment's filter operator (via
 * {@link FilterPlanNode}) and always resolves group-by keys by scanning the matched documents (there
 * is no metadata shortcut for grouped results). Mirrors Pinot's {@code GroupByPlanNode}, which also
 * constructs its own {@code FilterPlanNode} inside {@code run()}.
 */
public final class GroupByPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;

  public GroupByPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
  }

  @Override
  public Operator<DataTable> run() {
    List<AggregationFunction> functions =
        AggregationFunctionFactory.getAggregationFunctions(_queryContext);
    BaseFilterOperator filterOperator = new FilterPlanNode(_segmentContext, _queryContext).run();
    List<String> groupByColumns = new ArrayList<>();
    for (ExpressionContext expression : _queryContext.getGroupByExpressions()) {
      groupByColumns.add(expression.getIdentifier());
    }
    return new GroupByOperator(groupByColumns, functions, filterOperator, _indexSegment);
  }
}
