package io.minipinot.core.request;

import java.util.Objects;

/**
 * The unit of a query's expression language. Every select item, filter operand, group-by key and
 * order-by key is an {@code ExpressionContext}, which is exactly one of:
 * <ul>
 *   <li>{@link Type#IDENTIFIER} — a column reference,</li>
 *   <li>{@link Type#LITERAL} — a constant,</li>
 *   <li>{@link Type#FUNCTION} — an aggregation or transform.</li>
 * </ul>
 * Mirrors Pinot's {@code org.apache.pinot.common.request.context.ExpressionContext}.
 */
public final class ExpressionContext {
  public enum Type {
    LITERAL, IDENTIFIER, FUNCTION
  }

  private final Type _type;
  private final String _identifier;
  private final LiteralContext _literal;
  private final FunctionContext _function;

  private ExpressionContext(Type type, String identifier, LiteralContext literal,
      FunctionContext function) {
    _type = type;
    _identifier = identifier;
    _literal = literal;
    _function = function;
  }

  public static ExpressionContext forIdentifier(String identifier) {
    return new ExpressionContext(Type.IDENTIFIER, identifier, null, null);
  }

  public static ExpressionContext forLiteral(LiteralContext literal) {
    return new ExpressionContext(Type.LITERAL, null, literal, null);
  }

  public static ExpressionContext forFunction(FunctionContext function) {
    return new ExpressionContext(Type.FUNCTION, null, null, function);
  }

  public Type getType() {
    return _type;
  }

  public String getIdentifier() {
    return _identifier;
  }

  public LiteralContext getLiteral() {
    return _literal;
  }

  public FunctionContext getFunction() {
    return _function;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExpressionContext)) {
      return false;
    }
    ExpressionContext that = (ExpressionContext) o;
    return _type == that._type && Objects.equals(_identifier, that._identifier)
        && Objects.equals(_literal, that._literal) && Objects.equals(_function, that._function);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _identifier, _literal, _function);
  }

  @Override
  public String toString() {
    switch (_type) {
      case IDENTIFIER:
        return _identifier;
      case LITERAL:
        return _literal.toString();
      default:
        return _function.toString();
    }
  }
}
