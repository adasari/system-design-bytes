package io.minipinot.core.operator;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.combine.BaseCombineOperator;
import io.minipinot.core.request.QueryContext;
import java.util.List;

/**
 * The top per-instance operator. It drives the {@link BaseCombineOperator} (which merges this
 * instance's segments) and produces the single instance-level {@link DataTable} that the broker will
 * reduce across instances. Mirrors Pinot's {@code InstanceResponseOperator}, which wraps the combine
 * operator's block into an {@code InstanceResponseBlock} (Pinot also attaches CPU/memory metadata
 * here — omitted in MiniPinot).
 *
 * <p>Its only child is the combine operator, so the full operator tree is
 * instance-response -> combine -> segment operators, walkable via {@link #getChildOperators()}.
 */
public final class InstanceResponseOperator extends BaseOperator<DataTable> {
  private static final String EXPLAIN_NAME = "INSTANCE_RESPONSE";

  private final BaseCombineOperator _combineOperator;
  private final QueryContext _queryContext;

  public InstanceResponseOperator(BaseCombineOperator combineOperator, QueryContext queryContext) {
    _combineOperator = combineOperator;
    _queryContext = queryContext;
  }

  @Override
  protected DataTable getNextBlock() {
    return _combineOperator.nextBlock();
  }

  @Override
  public List<Operator<DataTable>> getChildOperators() {
    return List.of(_combineOperator);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }
}
