package io.minipinot.core.query.reduce;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.request.QueryContext;
import java.util.List;

/**
 * Merges the per-segment {@link DataTable}s of one query kind into the final {@link ResultTable}.
 * Mirrors Pinot's {@code DataTableReducer} family (aggregation / group-by / selection reducers), the
 * broker-side half of scatter-gather.
 */
public interface DataTableReducer {

  ResultTable reduce(List<DataTable> dataTables, QueryContext query,
      List<AggregationFunction> functions);
}
