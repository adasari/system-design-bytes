package io.minipinot.core.operator.query;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.BaseOperator;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.segment.DataSource;
import io.minipinot.segment.IndexSegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Answers an aggregation entirely from segment/column metadata, touching zero documents. It is used
 * only when the whole segment is matched (no {@code WHERE}) and every function is metadata-based
 * ({@code COUNT} from {@code totalDocs}, {@code MIN}/{@code MAX} from the sorted dictionary's
 * first/last value). Mirrors Pinot's {@code NonScanBasedAggregationOperator}.
 *
 * <p>Crucially it returns accumulators of the <em>same shape</em> as {@link AggregationOperator}, so
 * the broker reduce merges the two paths identically.
 *
 * <p><b>Example</b> -- {@code SELECT count(*), max(clicks) FROM events} (no {@code WHERE}) on a
 * segment of 5 docs whose {@code clicks} range over 1..5. No document is read:
 * <pre>{@code
 *   DataTable.forAggregation(
 *       ["count(*)","max(clicks)"],
 *       intermediates = [ 5L,     // COUNT from totalDocs
 *                         5.0 ]); // MAX from the clicks dictionary's last (largest) value
 * }</pre>
 */
public final class NonScanBasedAggregationOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "AGGREGATE_NO_SCAN";

  private final List<AggregationFunction> _functions;
  private final IndexSegment _segment;

  public NonScanBasedAggregationOperator(List<AggregationFunction> functions, IndexSegment segment) {
    _functions = functions;
    _segment = segment;
  }

  @Override
  protected DataTable getNextBlock() {
    int numTotalDocs = _segment.getTotalDocCount();
    int numFunctions = _functions.size();
    Object[] accumulators = new Object[numFunctions];
    List<String> columnNames = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      AggregationFunction function = _functions.get(i);
      columnNames.add(function.getResultName());
      DataSource dataSource = function.needsInputValue()
          ? _segment.getDataSource(function.getInputExpression().getIdentifier()) : null;
      accumulators[i] = function.aggregateFromMetadata(dataSource, numTotalDocs);
    }
    return DataTable.forAggregation(columnNames, accumulators);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
