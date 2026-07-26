package io.minipinot.core.plan.maker;

import io.minipinot.core.plan.AggregationPlanNode;
import io.minipinot.core.plan.CombinePlanNode;
import io.minipinot.core.plan.GlobalPlanImplV0;
import io.minipinot.core.plan.GroupByPlanNode;
import io.minipinot.core.plan.InstanceResponsePlanNode;
import io.minipinot.core.plan.Plan;
import io.minipinot.core.plan.PlanNode;
import io.minipinot.core.plan.SelectionPlanNode;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.SegmentContext;
import java.util.ArrayList;
import java.util.List;

/**
 * The default {@link PlanMaker}, mirroring Pinot's {@code InstancePlanMakerImplV2}. Its
 * {@link #makeSegmentPlanNode(SegmentContext, QueryContext)} dispatches on the query shape in exactly
 * the same order as Pinot:
 * <ol>
 *   <li>aggregation query with group-by expressions -&gt; {@link GroupByPlanNode},</li>
 *   <li>aggregation query without group-by -&gt; {@link AggregationPlanNode},</li>
 *   <li>otherwise (selection) -&gt; {@link SelectionPlanNode}.</li>
 * </ol>
 *
 * <p>Pinot has a fourth {@code DistinctPlanNode} branch; MiniPinot has no {@code DISTINCT} yet, so it
 * is omitted. Each plan node receives the {@link SegmentContext} and builds its own filter operator
 * internally, just like Pinot.
 *
 * <p>{@link #makeInstancePlan(List, QueryContext)} wraps the per-segment plan nodes in a
 * {@link CombinePlanNode} and an {@link InstanceResponsePlanNode}, returning a
 * {@link GlobalPlanImplV0} — the same shape Pinot's {@code makeInstancePlan} produces.
 */
public final class InstancePlanMakerImplV2 implements PlanMaker {

  @Override
  public PlanNode makeSegmentPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    if (queryContext.isAggregationQuery()) {
      if (queryContext.isGroupByQuery()) {
        return new GroupByPlanNode(segmentContext, queryContext);
      }
      return new AggregationPlanNode(segmentContext, queryContext);
    }
    return new SelectionPlanNode(segmentContext, queryContext);
  }

  @Override
  public Plan makeInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext) {
    List<PlanNode> planNodes = new ArrayList<>(segmentContexts.size());
    for (SegmentContext segmentContext : segmentContexts) {
      planNodes.add(makeSegmentPlanNode(segmentContext, queryContext));
    }
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext);
    return new GlobalPlanImplV0(
        new InstanceResponsePlanNode(combinePlanNode, segmentContexts, queryContext));
  }
}
