package io.minipinot.core.operator.combine;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.common.Operator;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.aggregation.function.AggregationFunctionFactory;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Combines the per-segment results of a non-grouped aggregation: it merges each function's
 * intermediate accumulator across the instance's segments, leaving the values <em>intermediate</em>
 * (the broker finalizes them later). Mirrors Pinot's {@code AggregationCombineOperator}.
 */
public final class AggregationCombineOperator extends BaseCombineOperator {

  public AggregationCombineOperator(List<Operator<DataTable>> operators, QueryContext queryContext) {
    super(operators, queryContext);
  }

  @Override
  protected DataTable mergeResults(List<DataTable> blocks) {
    // Example -- SELECT count(*), sum(clicks) FROM events over two segments:
    //   seg1 intermediates = [5L, 12.0]
    //   seg2 intermediates = [5L,  9.0]
    //   merged             = [ COUNT.merge(5,5)=10L, SUM.merge(12.0,9.0)=21.0 ]
    // The values stay intermediate; the broker's AggregationDataTableReducer calls
    // extractFinalResult later (for AVG that is where AvgPair(sum,count) becomes sum/count).
    List<AggregationFunction> functions =
        AggregationFunctionFactory.getAggregationFunctions(_queryContext);
    int numFunctions = functions.size();

    List<String> columnNames = new ArrayList<>(numFunctions);
    for (AggregationFunction function : functions) {
      columnNames.add(function.getResultName());
    }

    Object[] merged = new Object[numFunctions];
    boolean first = true;
    for (DataTable block : blocks) {
      Object[] intermediates = block.getAggregationIntermediates();
      if (first) {
        System.arraycopy(intermediates, 0, merged, 0, numFunctions);
        first = false;
      } else {
        for (int i = 0; i < numFunctions; i++) {
          merged[i] = functions.get(i).merge(merged[i], intermediates[i]);
        }
      }
    }
    if (first) {
      for (int i = 0; i < numFunctions; i++) {
        merged[i] = functions.get(i).createAccumulator();
      }
    }
    return DataTable.forAggregation(columnNames, merged);
  }
}
