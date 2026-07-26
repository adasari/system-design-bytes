package io.minipinot.core.request.predicate;

import io.minipinot.core.request.ExpressionContext;
import java.util.List;

/** {@code col IN (v1, v2, ...)}. Mirrors Pinot's {@code InPredicate}. */
public final class InPredicate extends Predicate {
  private final List<String> _values;

  public InPredicate(ExpressionContext lhs, List<String> values) {
    super(lhs);
    _values = values;
  }

  public List<String> getValues() {
    return _values;
  }

  @Override
  public Type getType() {
    return Type.IN;
  }

  @Override
  public String toString() {
    return _lhs + " IN " + _values;
  }
}
