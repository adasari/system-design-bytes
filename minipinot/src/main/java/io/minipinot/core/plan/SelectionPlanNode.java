package io.minipinot.core.plan;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.query.SelectionOnlyOperator;
import io.minipinot.core.operator.query.SelectionOrderByOperator;
import io.minipinot.core.request.ExpressionContext;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.IndexSegment;
import io.minipinot.segment.SegmentContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Plans a selection (projection) query over one segment. Expands {@code SELECT *} to the segment's
 * columns and chooses the ordered or unordered selection operator. To leave the broker enough rows to
 * apply the global {@code OFFSET}, each segment keeps up to {@code limit + offset} rows. Mirrors
 * Pinot's {@code SelectionPlanNode}, which likewise constructs its own {@code FilterPlanNode} inside
 * {@code run()}.
 */
public final class SelectionPlanNode implements PlanNode {
  private final IndexSegment _segment;
  private final SegmentContext _segmentContext;
  private final QueryContext _query;

  public SelectionPlanNode(SegmentContext segmentContext, QueryContext query) {
    _segment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _query = query;
  }

  @Override
  public Operator<DataTable> run() {
    BaseFilterOperator filterOperator = new FilterPlanNode(_segmentContext, _query).run();
    List<String> projectionColumns = resolveProjectionColumns();
    int numRowsToKeep = _query.getLimit() + _query.getOffset();
    if (_query.getOrderByExpressions() != null && !_query.getOrderByExpressions().isEmpty()) {
      return new SelectionOrderByOperator(projectionColumns, _query.getOrderByExpressions(),
          filterOperator, _segment, numRowsToKeep);
    }
    return new SelectionOnlyOperator(projectionColumns, filterOperator, _segment, numRowsToKeep);
  }

  private List<String> resolveProjectionColumns() {
    List<String> columns = new ArrayList<>();
    for (ExpressionContext expression : _query.getSelectExpressions()) {
      if (expression.getType() == ExpressionContext.Type.IDENTIFIER
          && "*".equals(expression.getIdentifier())) {
        return new ArrayList<>(_segment.getColumnNames());
      }
      columns.add(expression.getIdentifier());
    }
    return columns;
  }
}
