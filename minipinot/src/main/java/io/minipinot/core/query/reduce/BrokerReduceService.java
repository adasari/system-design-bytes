package io.minipinot.core.query.reduce;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.request.QueryContext;
import java.util.List;

/**
 * The broker-side entry point of scatter-gather reduce: it inspects the query shape and dispatches to
 * the matching {@link DataTableReducer}. Mirrors Pinot's {@code BrokerReduceService} +
 * {@code ResultReducerFactory}.
 */
public final class BrokerReduceService {

  private BrokerReduceService() {
  }

  public static ResultTable reduce(List<DataTable> dataTables, QueryContext query,
      List<AggregationFunction> functions) {
    DataTableReducer reducer;
    if (query.isGroupByQuery()) {
      reducer = new GroupByDataTableReducer();
    } else if (query.isAggregationQuery()) {
      reducer = new AggregationDataTableReducer();
    } else {
      reducer = new SelectionDataTableReducer();
    }
    return reducer.reduce(dataTables, query, functions);
  }
}
