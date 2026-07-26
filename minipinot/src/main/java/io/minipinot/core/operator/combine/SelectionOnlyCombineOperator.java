package io.minipinot.core.operator.combine;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.common.Operator;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Combines the per-segment results of a selection query with no {@code ORDER BY}: it concatenates the
 * segments' rows and keeps at most {@code limit + offset} of them, so the broker still has enough to
 * apply the global offset. Mirrors Pinot's {@code SelectionOnlyCombineOperator}.
 */
public final class SelectionOnlyCombineOperator extends BaseCombineOperator {

  public SelectionOnlyCombineOperator(List<Operator<DataTable>> operators, QueryContext queryContext) {
    super(operators, queryContext);
  }

  @Override
  protected DataTable mergeResults(List<DataTable> blocks) {
    // Example -- SELECT country, clicks FROM events LIMIT 3 over two segments:
    //   seg1 rows: [ ["US",3], ["IN",1] ]
    //   seg2 rows: [ ["FR",7], ["JP",2] ]
    //   merged   : [ ["US",3], ["IN",1], ["FR",7] ]   // concatenated, cut at limit+offset = 3
    // No order-by keys are carried (orderByKeys == null); the broker just applies OFFSET/LIMIT.
    List<String> columnNames = blocks.isEmpty()
        ? new ArrayList<>() : blocks.get(0).getColumnNames();
    int numRowsToKeep = _queryContext.getLimit() + _queryContext.getOffset();
    List<Object[]> rows = new ArrayList<>();
    for (DataTable block : blocks) {
      for (Object[] row : block.getRows()) {
        if (rows.size() >= numRowsToKeep) {
          break;
        }
        rows.add(row);
      }
      if (rows.size() >= numRowsToKeep) {
        break;
      }
    }
    return DataTable.forSelection(columnNames, rows, null);
  }
}
