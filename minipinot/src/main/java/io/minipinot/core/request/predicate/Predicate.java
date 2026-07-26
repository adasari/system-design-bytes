package io.minipinot.core.request.predicate;

import io.minipinot.core.request.ExpressionContext;

/**
 * Base class for leaf filter predicates. Every predicate has a left-hand-side
 * {@link ExpressionContext} (in MiniPinot always a column identifier) and a {@link Type}. Mirrors
 * Pinot's {@code org.apache.pinot.common.request.context.predicate.Predicate}.
 */
public abstract class Predicate {
  public enum Type {
    EQ, NOT_EQ, IN, RANGE
  }

  protected final ExpressionContext _lhs;

  protected Predicate(ExpressionContext lhs) {
    _lhs = lhs;
  }

  public abstract Type getType();

  public ExpressionContext getLhs() {
    return _lhs;
  }
}
