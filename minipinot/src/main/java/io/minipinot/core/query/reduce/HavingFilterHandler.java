package io.minipinot.core.query.reduce;

import io.minipinot.core.request.ExpressionContext;
import io.minipinot.core.request.FilterContext;
import io.minipinot.core.request.predicate.EqPredicate;
import io.minipinot.core.request.predicate.InPredicate;
import io.minipinot.core.request.predicate.NotEqPredicate;
import io.minipinot.core.request.predicate.Predicate;
import io.minipinot.core.request.predicate.RangePredicate;
import java.util.function.Function;

/**
 * Evaluates a {@code HAVING} clause against a single already-aggregated group. Unlike a {@code WHERE}
 * filter (which runs against a segment's dictionary ids), {@code HAVING} runs against concrete
 * aggregated <em>values</em> — an aggregation result such as {@code sum(clicks)} or a group-by
 * column. Mirrors Pinot's {@code HavingFilterHandler}.
 *
 * <p>The {@code resolver} maps a {@link ExpressionContext} (the predicate's left-hand side) to the
 * group's value for it. Numeric comparisons are done in {@code double} space; other types fall back
 * to string equality.
 */
public final class HavingFilterHandler {
  private final FilterContext _having;
  private final Function<ExpressionContext, Object> _resolver;

  public HavingFilterHandler(FilterContext having, Function<ExpressionContext, Object> resolver) {
    _having = having;
    _resolver = resolver;
  }

  public boolean isMatch() {
    return evaluate(_having);
  }

  private boolean evaluate(FilterContext filter) {
    switch (filter.getType()) {
      case AND:
        for (FilterContext child : filter.getChildren()) {
          if (!evaluate(child)) {
            return false;
          }
        }
        return true;
      case OR:
        for (FilterContext child : filter.getChildren()) {
          if (evaluate(child)) {
            return true;
          }
        }
        return false;
      case NOT:
        return !evaluate(filter.getChildren().get(0));
      case PREDICATE:
        return evaluatePredicate(filter.getPredicate());
      default:
        throw new IllegalStateException("Unhandled filter type: " + filter.getType());
    }
  }

  private boolean evaluatePredicate(Predicate predicate) {
    Object lhs = _resolver.apply(predicate.getLhs());
    switch (predicate.getType()) {
      case EQ:
        return valuesEqual(lhs, ((EqPredicate) predicate).getValue());
      case NOT_EQ:
        return !valuesEqual(lhs, ((NotEqPredicate) predicate).getValue());
      case IN:
        for (String value : ((InPredicate) predicate).getValues()) {
          if (valuesEqual(lhs, value)) {
            return true;
          }
        }
        return false;
      case RANGE:
        return inRange(lhs, (RangePredicate) predicate);
      default:
        throw new IllegalStateException("Unhandled predicate type: " + predicate.getType());
    }
  }

  private static boolean valuesEqual(Object lhs, String rhs) {
    if (lhs instanceof Number) {
      return ((Number) lhs).doubleValue() == Double.parseDouble(rhs);
    }
    return String.valueOf(lhs).equals(rhs);
  }

  private static boolean inRange(Object lhs, RangePredicate range) {
    double value = ((Number) lhs).doubleValue();
    if (!range.isLowerUnbounded()) {
      double lower = Double.parseDouble(range.getLowerBound());
      if (range.isLowerInclusive() ? value < lower : value <= lower) {
        return false;
      }
    }
    if (!range.isUpperUnbounded()) {
      double upper = Double.parseDouble(range.getUpperBound());
      if (range.isUpperInclusive() ? value > upper : value >= upper) {
        return false;
      }
    }
    return true;
  }
}
