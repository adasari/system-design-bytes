package io.minipinot.core.request;

import java.util.Objects;

/**
 * A resolved literal value with its (SQL) data type. Mirrors Pinot's
 * {@code org.apache.pinot.common.request.context.LiteralContext} in spirit: the parser produces
 * these for constants in the query, and the execution layer coerces them to the target column type.
 *
 * <p>MiniPinot keeps this deliberately small: the raw token text plus a flag telling whether it was
 * quoted (a string literal) or bare (a numeric literal). The actual coercion to a column's
 * {@link io.minipinot.spec.DataType} happens where the literal meets a column.
 */
public final class LiteralContext {
  private final String _value;
  private final boolean _string;

  public LiteralContext(String value, boolean string) {
    _value = value;
    _string = string;
  }

  public String getValue() {
    return _value;
  }

  public boolean isString() {
    return _string;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LiteralContext)) {
      return false;
    }
    LiteralContext that = (LiteralContext) o;
    return _string == that._string && _value.equals(that._value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_value, _string);
  }

  @Override
  public String toString() {
    return _string ? "'" + _value + "'" : _value;
  }
}
