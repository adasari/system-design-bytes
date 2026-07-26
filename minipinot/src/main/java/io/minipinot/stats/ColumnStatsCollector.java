package io.minipinot.stats;

import io.minipinot.spec.DataType;
import io.minipinot.spec.FieldSpec;
import java.util.TreeSet;

/**
 * Collects statistics for one column during the first pass over the input rows. Mirrors Pinot's
 * per-type {@code *ColumnPreIndexStatsCollector} (e.g. {@code IntColumnPreIndexStatsCollector}).
 *
 * <p>It maintains the set of distinct values in sorted order (which becomes the dictionary), the
 * min/max, whether the column is globally sorted, and multi-value bookkeeping.
 */
public final class ColumnStatsCollector {
  private final FieldSpec _fieldSpec;
  private final DataType _dataType;
  private final TreeSet<Comparable<?>> _uniqueValues = new TreeSet<>(NaturalOrder.INSTANCE);

  private Comparable<?> _previousValue;
  private boolean _sorted = true;
  private int _maxNumberOfMultiValues;
  private int _totalNumberOfEntries;
  private int _totalDocs;

  public ColumnStatsCollector(FieldSpec fieldSpec) {
    _fieldSpec = fieldSpec;
    _dataType = fieldSpec.getDataType();
  }

  /** Feed the value of this column from one document. */
  public void collect(Object value) {
    _totalDocs++;
    if (_fieldSpec.isSingleValue()) {
      Comparable<?> comparable = _dataType.toComparable(value);
      _uniqueValues.add(comparable);
      _totalNumberOfEntries++;
      if (_previousValue != null && NaturalOrder.INSTANCE.compare(comparable, _previousValue) < 0) {
        _sorted = false;
      }
      _previousValue = comparable;
    } else {
      Object[] values = (Object[]) value;
      _maxNumberOfMultiValues = Math.max(_maxNumberOfMultiValues, values.length);
      _totalNumberOfEntries += values.length;
      _sorted = false;
      for (Object element : values) {
        _uniqueValues.add(_dataType.toComparable(element));
      }
    }
  }

  public ColumnStats seal() {
    Comparable<?>[] sorted = _uniqueValues.toArray(new Comparable<?>[0]);
    Comparable<?> min = sorted.length > 0 ? sorted[0] : null;
    Comparable<?> max = sorted.length > 0 ? sorted[sorted.length - 1] : null;
    return new ColumnStats(sorted.length, sorted, min, max, _sorted && _fieldSpec.isSingleValue(),
        _fieldSpec.isSingleValue(), _maxNumberOfMultiValues, _totalNumberOfEntries, _totalDocs);
  }

  /** Natural ordering that works across the boxed types MiniPinot stores. */
  private enum NaturalOrder implements java.util.Comparator<Comparable<?>> {
    INSTANCE;

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int compare(Comparable<?> a, Comparable<?> b) {
      return ((Comparable) a).compareTo(b);
    }
  }
}
