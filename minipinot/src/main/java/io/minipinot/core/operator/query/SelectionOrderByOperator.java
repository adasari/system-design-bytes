package io.minipinot.core.operator.query;

import io.minipinot.core.common.Constants;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.BaseOperator;
import io.minipinot.core.operator.dociditerators.BlockDocIdIterator;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.projection.ColumnValueReader;
import io.minipinot.core.request.OrderByExpressionContext;
import io.minipinot.core.query.utils.ValueComparators;
import io.minipinot.segment.IndexSegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Selection with {@code ORDER BY}: keeps the local top {@code (limit + offset)} rows using a bounded
 * heap ordered by the order-by keys, so each segment ships only what the broker might need. Mirrors
 * Pinot's {@code SelectionOrderByOperator}.
 *
 * <p>Each retained row carries its parallel order-by key array so the broker can merge the
 * per-segment top-N lists into a single global order without re-reading the segments.
 *
 * <p><b>Example</b> -- {@code SELECT country, clicks FROM events ORDER BY clicks DESC LIMIT 2} on a
 * segment whose five rows have {@code clicks = 3,1,5,2,4}. The bounded heap keeps this segment's
 * local top 2 by clicks and emits them sorted, with parallel keys:
 * <pre>{@code
 *   DataTable.forSelection(
 *       ["country","clicks"],
 *       rows        = [ ["IN",5], ["US",4] ],   // the two largest clicks in this segment
 *       orderByKeys = [ [5],      [4]      ]);   // parallel to rows; drives the cross-segment merge-sort
 * }</pre>
 */
public final class SelectionOrderByOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "SELECT_ORDER_BY";

  private final List<String> _projectionColumns;
  private final List<OrderByExpressionContext> _orderByExpressions;
  private final BaseFilterOperator _filterOperator;
  private final IndexSegment _segment;
  private final int _numRowsToKeep;

  public SelectionOrderByOperator(List<String> projectionColumns,
      List<OrderByExpressionContext> orderByExpressions, BaseFilterOperator filterOperator,
      IndexSegment segment, int numRowsToKeep) {
    _projectionColumns = projectionColumns;
    _orderByExpressions = orderByExpressions;
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
    int numOrderBy = _orderByExpressions.size();
    ColumnValueReader[] orderReaders = new ColumnValueReader[numOrderBy];
    for (int i = 0; i < numOrderBy; i++) {
      String column = _orderByExpressions.get(i).getExpression().getIdentifier();
      orderReaders[i] = new ColumnValueReader(_segment.getDataSource(column));
    }

    Comparator<Object[]> keyComparator = ValueComparators.orderByComparator(_orderByExpressions);
    // Max-heap on the order-by key: the head is the worst row currently kept, evicted when full.
    PriorityQueue<Entry> heap =
        new PriorityQueue<>((a, b) -> -keyComparator.compare(a._key, b._key));

    BlockDocIdIterator iterator = _filterOperator.getDocIds().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      Object[] key = new Object[numOrderBy];
      for (int i = 0; i < numOrderBy; i++) {
        key[i] = orderReaders[i].getValue(docId);
      }
      if (heap.size() < _numRowsToKeep) {
        heap.offer(new Entry(project(readers, docId, numColumns), key));
      } else if (keyComparator.compare(key, heap.peek()._key) < 0) {
        heap.poll();
        heap.offer(new Entry(project(readers, docId, numColumns), key));
      }
    }

    List<Entry> entries = new ArrayList<>(heap);
    entries.sort((a, b) -> keyComparator.compare(a._key, b._key));
    List<Object[]> rows = new ArrayList<>(entries.size());
    List<Object[]> orderByKeys = new ArrayList<>(entries.size());
    for (Entry entry : entries) {
      rows.add(entry._row);
      orderByKeys.add(entry._key);
    }
    return DataTable.forSelection(_projectionColumns, rows, orderByKeys);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  private static Object[] project(ColumnValueReader[] readers, int docId, int numColumns) {
    Object[] row = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      row[i] = readers[i].getValue(docId);
    }
    return row;
  }

  private static final class Entry {
    final Object[] _row;
    final Object[] _key;

    Entry(Object[] row, Object[] key) {
      _row = row;
      _key = key;
    }
  }
}
