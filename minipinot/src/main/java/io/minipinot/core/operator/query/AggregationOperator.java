package io.minipinot.core.operator.query;

import io.minipinot.core.common.Constants;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.BaseOperator;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.projection.ColumnValueReader;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.segment.IndexSegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Scans the filtered documents of one segment and folds each into every aggregation function's
 * accumulator. Produces one intermediate accumulator per function. Mirrors Pinot's
 * {@code AggregationOperator} (the non-grouped, scan-based aggregation path).
 *
 * <p><b>Example</b> -- {@code SELECT count(*), sum(clicks) FROM events} on a segment whose five rows
 * have {@code clicks = 3,1,5,2,4}. Each matched doc folds into both accumulators, giving:
 * <pre>{@code
 *   DataTable.forAggregation(
 *       ["count(*)","sum(clicks)"],
 *       intermediates = [ 5L, 15.0 ]);   // 3+1+5+2+4 = 15.0; still intermediate (not finalized)
 * }</pre>
 */
public final class AggregationOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "AGGREGATE";

  private final List<AggregationFunction> _functions;
  private final BaseFilterOperator _filterOperator;
  private final IndexSegment _segment;

  public AggregationOperator(List<AggregationFunction> functions, BaseFilterOperator filterOperator,
      IndexSegment segment) {
    _functions = functions;
    _filterOperator = filterOperator;
    _segment = segment;
  }

  @Override
  protected DataTable getNextBlock() {
    int numFunctions = _functions.size();
    Object[] accumulators = new Object[numFunctions];
    ColumnValueReader[] readers = new ColumnValueReader[numFunctions];
    List<String> columnNames = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      AggregationFunction function = _functions.get(i);
      accumulators[i] = function.createAccumulator();
      columnNames.add(function.getResultName());
      if (function.needsInputValue()) {
        String column = function.getInputExpression().getIdentifier();
        readers[i] = new ColumnValueReader(_segment.getDataSource(column));
      }
    }

    BlockDocIdIterator iterator = _filterOperator.getDocIds().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      for (int i = 0; i < numFunctions; i++) {
        Object value = readers[i] == null ? null : readers[i].getValue(docId);
        accumulators[i] = _functions.get(i).aggregate(accumulators[i], value);
      }
    }
    return DataTable.forAggregation(columnNames, accumulators);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
