package io.minipinot.core.request.predicate;

import io.minipinot.core.request.ExpressionContext;

/**
 * A range predicate covering {@code >}, {@code >=}, {@code <}, {@code <=} and {@code BETWEEN}.
 * Mirrors Pinot's {@code RangePredicate}: the bound values are strings, an unbounded side is marked
 * with {@link #UNBOUNDED} ({@code "*"}), and inclusivity is tracked per side.
 *
 * <p>Examples: {@code col > 5} -> lower="5", lowerInclusive=false, upper="*"; {@code col BETWEEN 1
 * AND 9} -> lower="1", upper="9", both inclusive.
 */
public final class RangePredicate extends Predicate {
  public static final String UNBOUNDED = "*";

  private final String _lowerBound;
  private final boolean _lowerInclusive;
  private final String _upperBound;
  private final boolean _upperInclusive;

  public RangePredicate(ExpressionContext lhs, String lowerBound, boolean lowerInclusive,
      String upperBound, boolean upperInclusive) {
    super(lhs);
    _lowerBound = lowerBound;
    _lowerInclusive = lowerInclusive;
    _upperBound = upperBound;
    _upperInclusive = upperInclusive;
  }

  public String getLowerBound() {
    return _lowerBound;
  }

  public boolean isLowerInclusive() {
    return _lowerInclusive;
  }

  public String getUpperBound() {
    return _upperBound;
  }

  public boolean isUpperInclusive() {
    return _upperInclusive;
  }

  public boolean isLowerUnbounded() {
    return UNBOUNDED.equals(_lowerBound);
  }

  public boolean isUpperUnbounded() {
    return UNBOUNDED.equals(_upperBound);
  }

  @Override
  public Type getType() {
    return Type.RANGE;
  }

  @Override
  public String toString() {
    return _lhs + " " + (_lowerInclusive ? "[" : "(") + _lowerBound + ", " + _upperBound
        + (_upperInclusive ? "]" : ")");
  }
}
