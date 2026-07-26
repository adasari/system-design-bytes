package io.minipinot.core.request;

import java.util.List;
import java.util.Objects;

/**
 * A function invocation in a query, e.g. {@code SUM(revenue)} or {@code COUNT(*)}. Mirrors Pinot's
 * {@code org.apache.pinot.common.request.context.FunctionContext}.
 *
 * <p>The {@link Type} distinguishes aggregation functions (which fold many rows into one value) from
 * transform functions (row-wise). MiniPinot only executes aggregations, but the type is kept so the
 * plan maker can tell selections from aggregations exactly as Pinot does.
 */
public final class FunctionContext {
  public enum Type {
    AGGREGATION, TRANSFORM
  }

  private final Type _type;
  private final String _functionName;
  private final List<ExpressionContext> _arguments;

  public FunctionContext(Type type, String functionName, List<ExpressionContext> arguments) {
    _type = type;
    _functionName = functionName;
    _arguments = arguments;
  }

  public Type getType() {
    return _type;
  }

  /** Canonical (lower-case) function name, e.g. {@code sum}, {@code count}. */
  public String getFunctionName() {
    return _functionName;
  }

  public List<ExpressionContext> getArguments() {
    return _arguments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FunctionContext)) {
      return false;
    }
    FunctionContext that = (FunctionContext) o;
    return _type == that._type && _functionName.equals(that._functionName)
        && _arguments.equals(that._arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _functionName, _arguments);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(_functionName).append('(');
    for (int i = 0; i < _arguments.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(_arguments.get(i));
    }
    return sb.append(')').toString();
  }
}
