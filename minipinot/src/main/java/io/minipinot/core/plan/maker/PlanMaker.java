package io.minipinot.core.plan.maker;

import io.minipinot.core.plan.Plan;
import io.minipinot.core.plan.PlanNode;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.SegmentContext;
import java.util.List;

/**
 * Builds the executable plan for a query. Mirrors Pinot's {@code PlanMaker} interface: it exposes
 * both {@link #makeSegmentPlanNode(SegmentContext, QueryContext)} (the plan for a single segment) and
 * {@link #makeInstancePlan(List, QueryContext)} (the whole-instance plan that combines all segments).
 * In Pinot the concrete implementation is {@code InstancePlanMakerImplV2}; MiniPinot keeps the same
 * interface / impl split so the dispatch that selects group-by vs. aggregation vs. selection — and
 * the combine + instance-response wrapping — live behind a stable seam.
 */
public interface PlanMaker {

  /**
   * Makes the segment-level plan node for the given segment and query.
   */
  PlanNode makeSegmentPlanNode(SegmentContext segmentContext, QueryContext queryContext);

  /**
   * Makes the instance-level plan: one segment plan node per segment, combined by a
   * {@code CombinePlanNode} and wrapped in an {@code InstanceResponsePlanNode}.
   */
  Plan makeInstancePlan(List<SegmentContext> segmentContexts, QueryContext queryContext);
}
