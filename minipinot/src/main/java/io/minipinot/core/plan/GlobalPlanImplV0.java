package io.minipinot.core.plan;

import io.minipinot.core.datatable.DataTable;

/**
 * The global (instance-level) plan applied to all of an instance's segments. It runs the
 * {@link InstanceResponsePlanNode} to get the {@code InstanceResponseOperator} and executes it to
 * produce the instance response. Mirrors Pinot's {@code GlobalPlanImplV0}.
 */
public final class GlobalPlanImplV0 implements Plan {
  private final InstanceResponsePlanNode _instanceResponsePlanNode;

  public GlobalPlanImplV0(InstanceResponsePlanNode instanceResponsePlanNode) {
    _instanceResponsePlanNode = instanceResponsePlanNode;
  }

  @Override
  public PlanNode getPlanNode() {
    return _instanceResponsePlanNode;
  }

  @Override
  public DataTable execute() {
    return _instanceResponsePlanNode.run().nextBlock();
  }
}
