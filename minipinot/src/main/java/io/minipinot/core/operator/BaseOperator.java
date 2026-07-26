package io.minipinot.core.operator;

import io.minipinot.core.common.Block;
import io.minipinot.core.common.Operator;
import java.util.List;

/**
 * Base class every MiniPinot {@link Operator} should extend. It fixes the {@link #nextBlock()} entry
 * point as {@code final} and delegates the real work to {@link #getNextBlock()} — the same seam
 * Pinot's {@code BaseOperator} uses so cross-cutting concerns (tracing, early-termination checks,
 * resource sampling) can live in one place. MiniPinot runs single-threaded in one JVM, so that seam
 * is currently a plain pass-through, but keeping it makes the operator contract identical to Pinot's.
 *
 * @param <T> the type of {@link Block} this operator produces
 */
public abstract class BaseOperator<T extends Block> implements Operator<T> {

  @Override
  public final T nextBlock() {
    // In Pinot this is where the tracing scope + termination check wrap getNextBlock(); MiniPinot
    // keeps the seam but does no extra work.
    return getNextBlock();
  }

  /** Do the actual work. Protected so callers always go through {@link #nextBlock()}. */
  protected abstract T getNextBlock();

  @Override
  public List<? extends Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public String toExplainString() {
    return getClass().getSimpleName();
  }
}
