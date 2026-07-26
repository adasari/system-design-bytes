package io.minipinot.core.request.predicate;

import io.minipinot.core.request.ExpressionContext;

/** {@code col = value}. Mirrors Pinot's {@code EqPredicate}. */
public final class EqPredicate extends Predicate {
  private final String _value;

  public EqPredicate(ExpressionContext lhs, String value) {
    super(lhs);
    _value = value;
  }

  public String getValue() {
    return _value;
  }

  @Override
  public Type getType() {
    return Type.EQ;
  }

  @Override
  public String toString() {
    return _lhs + " = '" + _value + "'";
  }
}
