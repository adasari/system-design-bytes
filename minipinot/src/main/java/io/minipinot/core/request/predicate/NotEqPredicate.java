package io.minipinot.core.request.predicate;

import io.minipinot.core.request.ExpressionContext;

/** {@code col != value}. Mirrors Pinot's {@code NotEqPredicate}. */
public final class NotEqPredicate extends Predicate {
  private final String _value;

  public NotEqPredicate(ExpressionContext lhs, String value) {
    super(lhs);
    _value = value;
  }

  public String getValue() {
    return _value;
  }

  @Override
  public Type getType() {
    return Type.NOT_EQ;
  }

  @Override
  public String toString() {
    return _lhs + " != '" + _value + "'";
  }
}
