package io.minipinot.core.request;

import io.minipinot.core.request.predicate.Predicate;
import java.util.List;

/**
 * The tree form of a {@code WHERE} clause. An internal node is an {@link Type#AND}, {@link Type#OR}
 * or {@link Type#NOT} with children; a leaf is a {@link Type#PREDICATE} wrapping a {@link Predicate}.
 * Mirrors Pinot's {@code org.apache.pinot.common.request.context.FilterContext}.
 */
public final class FilterContext {
  public enum Type {
    AND, OR, NOT, PREDICATE
  }

  private final Type _type;
  private final List<FilterContext> _children;
  private final Predicate _predicate;

  private FilterContext(Type type, List<FilterContext> children, Predicate predicate) {
    _type = type;
    _children = children;
    _predicate = predicate;
  }

  public static FilterContext forAnd(List<FilterContext> children) {
    return new FilterContext(Type.AND, children, null);
  }

  public static FilterContext forOr(List<FilterContext> children) {
    return new FilterContext(Type.OR, children, null);
  }

  public static FilterContext forNot(FilterContext child) {
    return new FilterContext(Type.NOT, List.of(child), null);
  }

  public static FilterContext forPredicate(Predicate predicate) {
    return new FilterContext(Type.PREDICATE, null, predicate);
  }

  public Type getType() {
    return _type;
  }

  public List<FilterContext> getChildren() {
    return _children;
  }

  public Predicate getPredicate() {
    return _predicate;
  }

  @Override
  public String toString() {
    switch (_type) {
      case PREDICATE:
        return _predicate.toString();
      case NOT:
        return "NOT(" + _children.get(0) + ")";
      default:
        return _type + _children.toString();
    }
  }
}
