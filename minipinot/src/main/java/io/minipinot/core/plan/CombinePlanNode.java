package io.minipinot.core.plan;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.combine.AggregationCombineOperator;
import io.minipinot.core.operator.combine.BaseCombineOperator;
import io.minipinot.core.operator.combine.GroupByCombineOperator;
import io.minipinot.core.operator.combine.SelectionOnlyCombineOperator;
import io.minipinot.core.operator.combine.SelectionOrderByCombineOperator;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs each segment's {@link PlanNode} to get its {@link Operator}, then wraps them in the combine
 * operator that matches the query shape. This is the <em>server-side</em> combine step — it merges an
 * instance's segments, a level below the broker reduce. Mirrors Pinot's {@code CombinePlanNode},
 * whose {@code run()} selects among aggregation / group-by / selection-only / selection-order-by
 * combine operators in this same order (Pinot additionally handles streaming selection and DISTINCT,
 * which MiniPinot has no equivalents for yet).
 */
public final class CombinePlanNode implements PlanNode {
  private final List<PlanNode> _planNodes;
  private final QueryContext _queryContext;

  public CombinePlanNode(List<PlanNode> planNodes, QueryContext queryContext) {
    _planNodes = planNodes;
    _queryContext = queryContext;
  }

  @Override
  public BaseCombineOperator run() {
    List<Operator<DataTable>> operators = new ArrayList<>(_planNodes.size());
    for (PlanNode planNode : _planNodes) {
      operators.add(planNode.run());
    }
    if (_queryContext.isAggregationQuery()) {
      if (_queryContext.isGroupByQuery()) {
        return new GroupByCombineOperator(operators, _queryContext);
      }
      return new AggregationCombineOperator(operators, _queryContext);
    }
    if (_queryContext.getLimit() == 0 || _queryContext.getOrderByExpressions() == null
        || _queryContext.getOrderByExpressions().isEmpty()) {
      return new SelectionOnlyCombineOperator(operators, _queryContext);
    }
    return new SelectionOrderByCombineOperator(operators, _queryContext);
  }
}
