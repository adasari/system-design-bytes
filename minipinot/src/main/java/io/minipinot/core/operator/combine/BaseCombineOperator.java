package io.minipinot.core.operator.combine;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.BaseOperator;
import io.minipinot.core.request.QueryContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Base of the <em>server-side</em> combine operators. It runs every per-segment {@link Operator} to
 * get that segment's intermediate {@link DataTable} (via {@code nextBlock()}), then folds them into a
 * single instance-level intermediate {@link DataTable} via {@link #mergeResults(List)}. Mirrors
 * Pinot's {@code BaseCombineOperator}, whose {@code getNextBlock()} runs the segment operators (in
 * Pinot, across a thread pool) and merges their {@code IntermediateResultsBlock}s into one instance
 * block.
 *
 * <p>This is a distinct merge level from the broker's {@code DataTableReducer}: combine merges the
 * segments <em>within one server instance</em> and still yields <em>intermediate</em> (not finalized)
 * results; the broker later merges across instances and finalizes. MiniPinot runs single-threaded in
 * one JVM, so the parallelism/timeout machinery of Pinot's combine is intentionally omitted.
 *
 * <p>The per-segment operators are exposed as this operator's children via {@link
 * #getChildOperators()}, so the operator tree (instance-response -> combine -> segment operators) can
 * be walked exactly like Pinot's.
 */
public abstract class BaseCombineOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "COMBINE";

  protected final List<Operator<DataTable>> _operators;
  protected final QueryContext _queryContext;

  protected BaseCombineOperator(List<Operator<DataTable>> operators, QueryContext queryContext) {
    _operators = operators;
    _queryContext = queryContext;
  }

  @Override
  protected DataTable getNextBlock() {
    List<DataTable> blocks = new ArrayList<>(_operators.size());
    for (Operator<DataTable> operator : _operators) {
      blocks.add(operator.nextBlock());
    }
    return mergeResults(blocks);
  }

  @Override
  public List<Operator<DataTable>> getChildOperators() {
    return _operators;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  /** Merge the per-segment intermediate results of this instance into one intermediate result. */
  protected abstract DataTable mergeResults(List<DataTable> blocks);
}
