package io.minipinot.core.datatable;

import io.minipinot.core.common.Block;
import java.util.List;
import java.util.Map;

/**
 * The intermediate result payload passed up the operator tree and, ultimately, to the broker for
 * reduction. In Pinot this is a serialized {@code DataTable} sent over the wire; MiniPinot runs in
 * one JVM so it is a plain in-memory object, but it plays the same role: it carries
 * <em>intermediate</em> (not yet finalized) results so they can be merged correctly.
 *
 * <p>The same shape is produced at two levels: each segment {@link
 * io.minipinot.core.common.Operator} emits one for its documents (via {@code nextBlock()}), and the
 * server-side combine operator ({@link io.minipinot.core.operator.combine.BaseCombineOperator})
 * merges an instance's segment tables into a single instance-level table. The broker then reduces the
 * per-instance tables and finalizes.
 *
 * <p>There are three shapes, matching the three query kinds:
 * <ul>
 *   <li>{@link Type#SELECTION}: raw projected rows (plus parallel order-by keys when ordered),</li>
 *   <li>{@link Type#AGGREGATION}: one intermediate accumulator per aggregation function,</li>
 *   <li>{@link Type#GROUP_BY}: a map from group key (list of group-column values) to the group's
 *       intermediate accumulators.</li>
 * </ul>
 *
 * <p><b>Examples</b> (sample {@code events} table with columns {@code country, clicks, revenue}).
 * These are exactly what one segment operator emits and what the combine operators merge:
 * <pre>{@code
 * // SELECTION: SELECT country, clicks FROM events LIMIT 10   (no ORDER BY -> orderByKeys == null)
 * DataTable.forSelection(
 *     columnNames = ["country", "clicks"],
 *     rows        = [ ["US", 3], ["IN", 1] ],
 *     orderByKeys = null);
 *
 * // SELECTION ordered: SELECT country, clicks FROM events ORDER BY clicks DESC LIMIT 2
 * DataTable.forSelection(
 *     columnNames = ["country", "clicks"],
 *     rows        = [ ["FR", 7], ["US", 3] ],   // this segment's local top-N, already sorted
 *     orderByKeys = [ [7],       [3]       ]);   // parallel to rows; drives the cross-segment merge-sort
 *
 * // AGGREGATION: SELECT count(*), sum(clicks), avg(revenue) FROM events
 * DataTable.forAggregation(
 *     columnNames   = ["count(*)", "sum(clicks)", "avg(revenue)"],
 *     intermediates = [ 5L,        12.0,          AvgPair(sum=40.0, count=5) ]);
 *     // note: AVG carries the (sum, count) pair, NOT the final 8.0 -- finalized only in the reducer.
 *
 * // GROUP_BY: SELECT country, count(*) FROM events GROUP BY country
 * DataTable.forGroupBy(
 *     columnNames = ["count(*)"],
 *     groups      = { ["US"] -> [3L], ["IN"] -> [2L] });   // key = list of group-column values
 * }</pre>
 */
public final class DataTable implements Block {
  public enum Type {
    SELECTION, AGGREGATION, GROUP_BY
  }

  private final Type _type;
  private final List<String> _columnNames;
  private final Object[] _aggregationIntermediates;
  private final Map<List<Object>, Object[]> _groups;
  private final List<Object[]> _rows;
  private final List<Object[]> _orderByKeys;

  private DataTable(Type type, List<String> columnNames, Object[] aggregationIntermediates,
      Map<List<Object>, Object[]> groups, List<Object[]> rows, List<Object[]> orderByKeys) {
    _type = type;
    _columnNames = columnNames;
    _aggregationIntermediates = aggregationIntermediates;
    _groups = groups;
    _rows = rows;
    _orderByKeys = orderByKeys;
  }

  public static DataTable forAggregation(List<String> columnNames, Object[] intermediates) {
    return new DataTable(Type.AGGREGATION, columnNames, intermediates, null, null, null);
  }

  public static DataTable forGroupBy(List<String> columnNames, Map<List<Object>, Object[]> groups) {
    return new DataTable(Type.GROUP_BY, columnNames, null, groups, null, null);
  }

  /** {@code orderByKeys} is {@code null} for unordered selection, else parallel to {@code rows}. */
  public static DataTable forSelection(List<String> columnNames, List<Object[]> rows,
      List<Object[]> orderByKeys) {
    return new DataTable(Type.SELECTION, columnNames, null, null, rows, orderByKeys);
  }

  public Type getType() {
    return _type;
  }

  public List<String> getColumnNames() {
    return _columnNames;
  }

  public Object[] getAggregationIntermediates() {
    return _aggregationIntermediates;
  }

  public Map<List<Object>, Object[]> getGroups() {
    return _groups;
  }

  public List<Object[]> getRows() {
    return _rows;
  }

  public List<Object[]> getOrderByKeys() {
    return _orderByKeys;
  }
}
