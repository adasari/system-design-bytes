package io.minipinot.core.operator.combine;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.common.Operator;
import io.minipinot.core.query.utils.ValueComparators;
import io.minipinot.core.request.OrderByExpressionContext;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Combines the per-segment results of a selection query with {@code ORDER BY}: it merge-sorts the
 * segments' local top-N lists using their carried order-by keys and keeps the top {@code limit +
 * offset} rows (with keys), so the broker can produce the global order across instances. Mirrors
 * Pinot's {@code SelectionOrderByCombineOperator}.
 */
public final class SelectionOrderByCombineOperator extends BaseCombineOperator {

  public SelectionOrderByCombineOperator(List<Operator<DataTable>> operators, QueryContext queryContext) {
    super(operators, queryContext);
  }

  @Override
  protected DataTable mergeResults(List<DataTable> blocks) {
    // Example -- SELECT country, clicks FROM events ORDER BY clicks DESC LIMIT 2 over two segments:
    //   seg1 rows/keys: [ ["FR",7]/[7], ["US",3]/[3] ]   // each segment is already locally top-N sorted
    //   seg2 rows/keys: [ ["IN",5]/[5], ["JP",1]/[1] ]
    //   merge-sort by key DESC, keep top (limit+offset)=2:
    //     merged rows = [ ["FR",7], ["IN",5] ], keys = [ [7], [5] ]
    // The keys are kept parallel to the rows so the broker can merge-sort again across instances.
    List<String> columnNames = blocks.isEmpty()
        ? new ArrayList<>() : blocks.get(0).getColumnNames();
    List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();

    List<Object[]> rows = new ArrayList<>();
    List<Object[]> keys = new ArrayList<>();
    for (DataTable block : blocks) {
      List<Object[]> blockRows = block.getRows();
      List<Object[]> blockKeys = block.getOrderByKeys();
      for (int i = 0; i < blockRows.size(); i++) {
        rows.add(blockRows.get(i));
        keys.add(blockKeys.get(i));
      }
    }

    List<Integer> indices = new ArrayList<>(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      indices.add(i);
    }
    Comparator<Object[]> keyComparator = ValueComparators.orderByComparator(orderByExpressions);
    indices.sort((a, b) -> keyComparator.compare(keys.get(a), keys.get(b)));

    int numRowsToKeep = Math.min(_queryContext.getLimit() + _queryContext.getOffset(), rows.size());
    List<Object[]> sortedRows = new ArrayList<>(numRowsToKeep);
    List<Object[]> sortedKeys = new ArrayList<>(numRowsToKeep);
    for (int i = 0; i < numRowsToKeep; i++) {
      int index = indices.get(i);
      sortedRows.add(rows.get(index));
      sortedKeys.add(keys.get(index));
    }
    return DataTable.forSelection(columnNames, sortedRows, sortedKeys);
  }
}
