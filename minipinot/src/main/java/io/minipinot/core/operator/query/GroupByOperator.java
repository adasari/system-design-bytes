package io.minipinot.core.operator.query;

import io.minipinot.core.common.Constants;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.BaseOperator;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.projection.ColumnValueReader;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.segment.IndexSegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups the filtered documents of one segment by the group-by columns' <em>values</em> and folds
 * each group's documents into per-function accumulators. Mirrors Pinot's group-by execution
 * ({@code GroupByOperator} + {@code DefaultGroupByExecutor}), reduced to a plain hash map.
 *
 * <p>The key is the list of group-by column values (not dictIds — dictIds are segment-local, so
 * value-based keys are what makes cross-segment reduce correct). {@code byte[]} parts are normalized
 * to an ISO-8859-1 string so they hash/equal correctly.
 *
 * <p><b>Example</b> -- {@code SELECT country, count(*), sum(clicks) FROM events GROUP BY country} on
 * a segment holding {@code (US,3) (US,1) (IN,5) (IN,2) (US,4)} (country, clicks). Rows are grouped by
 * the country VALUE and folded into per-function accumulators:
 * <pre>{@code
 *   DataTable.forGroupBy(
 *       ["count(*)","sum(clicks)"],
 *       groups = { ["US"] -> [3L, 8.0],     // US clicks 3,1,4 -> count 3, sum 8.0
 *                  ["IN"] -> [2L, 7.0] });  // IN clicks 5,2   -> count 2, sum 7.0
 * }</pre>
 */
public final class GroupByOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "GROUP_BY";

  private final List<String> _groupByColumns;
  private final List<AggregationFunction> _functions;
  private final BaseFilterOperator _filterOperator;
  private final IndexSegment _segment;

  public GroupByOperator(List<String> groupByColumns, List<AggregationFunction> functions,
      BaseFilterOperator filterOperator, IndexSegment segment) {
    _groupByColumns = groupByColumns;
    _functions = functions;
    _filterOperator = filterOperator;
    _segment = segment;
  }

  @Override
  protected DataTable getNextBlock() {
    int numGroupBy = _groupByColumns.size();
    int numFunctions = _functions.size();

    ColumnValueReader[] groupReaders = new ColumnValueReader[numGroupBy];
    for (int i = 0; i < numGroupBy; i++) {
      groupReaders[i] = new ColumnValueReader(_segment.getDataSource(_groupByColumns.get(i)));
    }
    ColumnValueReader[] aggReaders = new ColumnValueReader[numFunctions];
    List<String> columnNames = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      AggregationFunction function = _functions.get(i);
      columnNames.add(function.getResultName());
      if (function.needsInputValue()) {
        aggReaders[i] = new ColumnValueReader(
            _segment.getDataSource(function.getInputExpression().getIdentifier()));
      }
    }

    Map<List<Object>, Object[]> groups = new LinkedHashMap<>();
    BlockDocIdIterator iterator = _filterOperator.getDocIds().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      List<Object> key = new ArrayList<>(numGroupBy);
      for (int i = 0; i < numGroupBy; i++) {
        key.add(normalizeKeyPart(groupReaders[i].getValue(docId)));
      }
      Object[] accumulators = groups.get(key);
      if (accumulators == null) {
        accumulators = new Object[numFunctions];
        for (int i = 0; i < numFunctions; i++) {
          accumulators[i] = _functions.get(i).createAccumulator();
        }
        groups.put(key, accumulators);
      }
      for (int i = 0; i < numFunctions; i++) {
        Object value = aggReaders[i] == null ? null : aggReaders[i].getValue(docId);
        accumulators[i] = _functions.get(i).aggregate(accumulators[i], value);
      }
    }
    return DataTable.forGroupBy(columnNames, groups);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /** Make a value usable as a hash-map key part ({@code byte[]} is not value-equal by default). */
  static Object normalizeKeyPart(Object value) {
    if (value instanceof byte[]) {
      return new String((byte[]) value, StandardCharsets.ISO_8859_1);
    }
    return value;
  }
}
