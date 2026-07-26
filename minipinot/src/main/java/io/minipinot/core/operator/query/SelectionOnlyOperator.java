package io.minipinot.core.operator.query;

import io.minipinot.core.common.Constants;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.BaseOperator;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.projection.ColumnValueReader;
import io.minipinot.segment.IndexSegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Selection without {@code ORDER BY}: projects the requested columns for matched documents and stops
 * once enough rows are gathered. It keeps up to {@code limit + offset} rows so the broker still has
 * enough to apply the global offset. Mirrors Pinot's {@code SelectionOnlyOperator}.
 *
 * <p><b>Example</b> -- {@code SELECT country, clicks FROM events LIMIT 10} on a segment holding:
 * <pre>{@code
 *   country browser  device  clicks
 *   US      Chrome   Desktop 3
 *   US      Safari   Mobile  1
 *   IN      Chrome   Mobile  5
 *   IN      Firefox  Desktop 2
 *   US      Chrome   Mobile  4
 *
 * execute() projects [country, clicks] for the matched docs (all 5; no WHERE), keeping up to
 * limit+offset rows, and emits:
 *   DataTable.forSelection(
 *       ["country","clicks"],
 *       rows        = [ ["US",3], ["US",1], ["IN",5], ["IN",2], ["US",4] ],
 *       orderByKeys = null);
 * }</pre>
 */
public final class SelectionOnlyOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "SELECT";

  private final List<String> _projectionColumns;
  private final BaseFilterOperator _filterOperator;
  private final IndexSegment _segment;
  private final int _numRowsToKeep;

  public SelectionOnlyOperator(List<String> projectionColumns, BaseFilterOperator filterOperator,
      IndexSegment segment, int numRowsToKeep) {
    _projectionColumns = projectionColumns;
    _filterOperator = filterOperator;
    _segment = segment;
    _numRowsToKeep = numRowsToKeep;
  }

  @Override
  protected DataTable getNextBlock() {
    int numColumns = _projectionColumns.size();
    ColumnValueReader[] readers = new ColumnValueReader[numColumns];
    for (int i = 0; i < numColumns; i++) {
      readers[i] = new ColumnValueReader(_segment.getDataSource(_projectionColumns.get(i)));
    }

    List<Object[]> rows = new ArrayList<>();
    BlockDocIdIterator iterator = _filterOperator.getDocIds().iterator();
    int docId;
    while (rows.size() < _numRowsToKeep && (docId = iterator.next()) != Constants.EOF) {
      Object[] row = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        row[i] = readers[i].getValue(docId);
      }
      rows.add(row);
    }
    return DataTable.forSelection(_projectionColumns, rows, null);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
