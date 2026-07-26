package io.minipinot.core.query.reduce;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Reduces a non-grouped aggregation: merge each function's intermediate accumulator across all
 * segments, then extract the final value into a single result row. Mirrors Pinot's
 * {@code AggregationDataTableReducer}.
 */
public final class AggregationDataTableReducer implements DataTableReducer {

  @Override
  public ResultTable reduce(List<DataTable> dataTables, QueryContext query,
      List<AggregationFunction> functions) {
    int numFunctions = functions.size();
    Object[] merged = new Object[numFunctions];
    List<String> columnNames = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      columnNames.add(functions.get(i).getResultName());
    }

    boolean first = true;
    for (DataTable dataTable : dataTables) {
      Object[] intermediates = dataTable.getAggregationIntermediates();
      if (first) {
        System.arraycopy(intermediates, 0, merged, 0, numFunctions);
        first = false;
      } else {
        for (int i = 0; i < numFunctions; i++) {
          merged[i] = functions.get(i).merge(merged[i], intermediates[i]);
        }
      }
    }

    Object[] row = new Object[numFunctions];
    for (int i = 0; i < numFunctions; i++) {
      row[i] = first ? functions.get(i).extractFinalResult(functions.get(i).createAccumulator())
          : functions.get(i).extractFinalResult(merged[i]);
    }
    List<Object[]> rows = new ArrayList<>(1);
    rows.add(row);
    return new ResultTable(columnNames, rows);
  }
}
