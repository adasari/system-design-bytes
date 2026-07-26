package io.minipinot.core.plan;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;

/**
 * A single node of a segment-level execution plan: calling {@link #run()} materializes the
 * executable {@link Operator} for this segment. Mirrors Pinot's
 * {@code org.apache.pinot.core.plan.PlanNode} (whose {@code run()} returns the segment operator).
 *
 * <p>In Pinot the plan maker builds a tree of {@code PlanNode}s (filter -> project -> aggregation/
 * selection) and {@code run()} wires the corresponding operators together. MiniPinot keeps the same
 * shape but a flatter tree: each query-kind plan node builds its own {@code FilterPlanNode} inside
 * {@code run()}, exactly as Pinot's {@code AggregationPlanNode}/{@code SelectionPlanNode} do.
 */
public interface PlanNode {

  Operator<DataTable> run();
}
