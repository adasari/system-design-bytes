package io.minipinot.core.query.aggregation.function;

/**
 * The intermediate result of {@code AVG}: a running sum and count. Keeping both (instead of a
 * premature ratio) is what lets partial averages from different segments be merged correctly.
 * Mirrors Pinot's {@code AvgPair}.
 */
public final class AvgPair {
  private double _sum;
  private long _count;

  public AvgPair(double sum, long count) {
    _sum = sum;
    _count = count;
  }

  public void apply(double value) {
    _sum += value;
    _count++;
  }

  public AvgPair merge(AvgPair other) {
    _sum += other._sum;
    _count += other._count;
    return this;
  }

  public double getSum() {
    return _sum;
  }

  public long getCount() {
    return _count;
  }

  public double toAverage() {
    return _count == 0 ? 0.0 : _sum / _count;
  }
}
