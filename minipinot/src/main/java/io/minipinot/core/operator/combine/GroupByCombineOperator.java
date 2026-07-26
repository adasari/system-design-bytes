package io.minipinot.core.operator.combine;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.common.Operator;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.aggregation.function.AggregationFunctionFactory;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Combines the per-segment results of a grouped aggregation: it merges the segments' group maps by
 * value-key, folding matching groups' accumulators together. The merged groups stay
 * <em>intermediate</em> (final extraction, {@code HAVING}, {@code ORDER BY} and {@code LIMIT} happen
 * in the broker reduce). Mirrors Pinot's {@code GroupByCombineOperator}.
 */
public final class GroupByCombineOperator extends BaseCombineOperator {

  public GroupByCombineOperator(List<Operator<DataTable>> operators, QueryContext queryContext) {
    super(operators, queryContext);
  }

  @Override
  protected DataTable mergeResults(List<DataTable> blocks) {
    // Example -- SELECT country, count(*) FROM events GROUP BY country over two segments:
    //   seg1 groups: { ["US"] -> [3L], ["IN"] -> [2L] }
    //   seg2 groups: { ["US"] -> [1L], ["FR"] -> [4L] }
    //   merged     : { ["US"] -> [4L], ["IN"] -> [2L], ["FR"] -> [4L] }
    // Groups are keyed by group-column VALUES (not segment-local dictIds), so "US" from different
    // segments lands on the same key and its accumulators are merged. Still intermediate: HAVING,
    // final extraction, ORDER BY and LIMIT all happen later in the broker reduce.
    List<AggregationFunction> functions =
        AggregationFunctionFactory.getAggregationFunctions(_queryContext);
    int numFunctions = functions.size();

    List<String> columnNames = null;
    Map<List<Object>, Object[]> merged = new LinkedHashMap<>();
    for (DataTable block : blocks) {
      if (columnNames == null) {
        columnNames = block.getColumnNames();
      }
      for (Map.Entry<List<Object>, Object[]> entry : block.getGroups().entrySet()) {
        Object[] accumulators = merged.get(entry.getKey());
        if (accumulators == null) {
          merged.put(entry.getKey(), entry.getValue().clone());
        } else {
          Object[] other = entry.getValue();
          for (int i = 0; i < numFunctions; i++) {
            accumulators[i] = functions.get(i).merge(accumulators[i], other[i]);
          }
        }
      }
    }
    if (columnNames == null) {
      columnNames = new ArrayList<>(numFunctions);
      for (AggregationFunction function : functions) {
        columnNames.add(function.getResultName());
      }
    }
    return DataTable.forGroupBy(columnNames, merged);
  }
}
