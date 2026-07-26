package io.minipinot.core.query.reduce;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.utils.ValueComparators;
import io.minipinot.core.request.OrderByExpressionContext;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reduces a selection (projection) query. For unordered selection it concatenates the per-segment
 * rows; for ordered selection it merge-sorts the per-segment top-N lists using their carried
 * order-by keys. Then it applies the global {@code OFFSET}/{@code LIMIT}. Mirrors Pinot's
 * {@code SelectionDataTableReducer} / {@code SelectionOperatorService}.
 */
public final class SelectionDataTableReducer implements DataTableReducer {

  @Override
  public ResultTable reduce(List<DataTable> dataTables, QueryContext query,
      List<AggregationFunction> functions) {
    List<String> columnNames = dataTables.isEmpty()
        ? new ArrayList<>() : dataTables.get(0).getColumnNames();
    List<OrderByExpressionContext> orderBys = query.getOrderByExpressions();
    boolean ordered = orderBys != null && !orderBys.isEmpty();

    List<Object[]> rows = new ArrayList<>();
    if (ordered) {
      List<Object[]> keys = new ArrayList<>();
      for (DataTable dataTable : dataTables) {
        List<Object[]> dtRows = dataTable.getRows();
        List<Object[]> dtKeys = dataTable.getOrderByKeys();
        for (int i = 0; i < dtRows.size(); i++) {
          rows.add(dtRows.get(i));
          keys.add(dtKeys.get(i));
        }
      }
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < rows.size(); i++) {
        indices.add(i);
      }
      Comparator<Object[]> keyComparator = ValueComparators.orderByComparator(orderBys);
      indices.sort((a, b) -> keyComparator.compare(keys.get(a), keys.get(b)));
      List<Object[]> sorted = new ArrayList<>(rows.size());
      for (int index : indices) {
        sorted.add(rows.get(index));
      }
      rows = sorted;
    } else {
      for (DataTable dataTable : dataTables) {
        rows.addAll(dataTable.getRows());
      }
    }

    int from = Math.min(query.getOffset(), rows.size());
    int to = Math.min(from + query.getLimit(), rows.size());
    return new ResultTable(columnNames, new ArrayList<>(rows.subList(from, to)));
  }
}
