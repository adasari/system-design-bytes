package io.minipinot.core.plan;

import io.minipinot.core.operator.InstanceResponseOperator;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.SegmentContext;
import java.util.List;

/**
 * The root plan node for one instance. It holds the {@link CombinePlanNode} for the instance's
 * segments and, on {@link #run()}, builds the {@link InstanceResponseOperator} over the combine
 * operator. Mirrors Pinot's {@code InstanceResponsePlanNode} (which also carries the segment /
 * fetch contexts used for prefetch — MiniPinot keeps only the segment contexts).
 */
public final class InstanceResponsePlanNode implements PlanNode {
  private final CombinePlanNode _combinePlanNode;
  private final List<SegmentContext> _segmentContexts;
  private final QueryContext _queryContext;

  public InstanceResponsePlanNode(CombinePlanNode combinePlanNode,
      List<SegmentContext> segmentContexts, QueryContext queryContext) {
    _combinePlanNode = combinePlanNode;
    _segmentContexts = segmentContexts;
    _queryContext = queryContext;
  }

  @Override
  public InstanceResponseOperator run() {
    return new InstanceResponseOperator(_combinePlanNode.run(), _queryContext);
  }
}
