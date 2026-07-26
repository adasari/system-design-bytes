package io.minipinot.stats;

/**
 * The immutable result of the stats-collection pass for a single column. These statistics drive
 * later phases: cardinality decides the forward-index bit width, {@code sorted} selects the
 * sorted forward index, and min/max feed segment metadata and range indexes.
 *
 * <p>Mirrors the subset of Pinot's {@code ColumnStatistics} /
 * {@code AbstractColumnStatisticsCollector} that MiniPinot needs.
 */
public final class ColumnStats {
  private final int _cardinality;
  private final Comparable<?>[] _sortedUniqueValues;
  private final Comparable<?> _minValue;
  private final Comparable<?> _maxValue;
  private final boolean _sorted;
  private final boolean _singleValue;
  private final int _maxNumberOfMultiValues;
  private final int _totalNumberOfEntries;
  private final int _totalDocs;

  public ColumnStats(int cardinality, Comparable<?>[] sortedUniqueValues, Comparable<?> minValue,
      Comparable<?> maxValue, boolean sorted, boolean singleValue, int maxNumberOfMultiValues,
      int totalNumberOfEntries, int totalDocs) {
    _cardinality = cardinality;
    _sortedUniqueValues = sortedUniqueValues;
    _minValue = minValue;
    _maxValue = maxValue;
    _sorted = sorted;
    _singleValue = singleValue;
    _maxNumberOfMultiValues = maxNumberOfMultiValues;
    _totalNumberOfEntries = totalNumberOfEntries;
    _totalDocs = totalDocs;
  }

  public int getCardinality() {
    return _cardinality;
  }

  public Comparable<?>[] getSortedUniqueValues() {
    return _sortedUniqueValues;
  }

  public Comparable<?> getMinValue() {
    return _minValue;
  }

  public Comparable<?> getMaxValue() {
    return _maxValue;
  }

  /** True when the column's single values appear in non-decreasing document order. */
  public boolean isSorted() {
    return _sorted;
  }

  public boolean isSingleValue() {
    return _singleValue;
  }

  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }

  public int getTotalDocs() {
    return _totalDocs;
  }
}
