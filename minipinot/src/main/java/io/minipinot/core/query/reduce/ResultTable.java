package io.minipinot.core.query.reduce;

import java.util.List;

/**
 * The final, user-facing query result: column names plus fully-materialized rows. This is what the
 * broker returns after reducing all per-segment {@code DataTable}s. Mirrors Pinot's
 * {@code org.apache.pinot.common.response.broker.ResultTable}.
 */
public final class ResultTable {
  private final List<String> _columnNames;
  private final List<Object[]> _rows;

  public ResultTable(List<String> columnNames, List<Object[]> rows) {
    _columnNames = columnNames;
    _rows = rows;
  }

  public List<String> getColumnNames() {
    return _columnNames;
  }

  public List<Object[]> getRows() {
    return _rows;
  }

  public int getNumRows() {
    return _rows.size();
  }
}
