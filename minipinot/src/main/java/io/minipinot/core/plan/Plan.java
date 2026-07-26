package io.minipinot.core.plan;

import io.minipinot.core.datatable.DataTable;

/**
 * An instance-level query plan: {@link #execute()} runs it and returns this instance's single
 * intermediate {@link DataTable} (the "instance response") for the broker to reduce. Mirrors Pinot's
 * {@code Plan} interface (whose {@code execute()} returns an {@code InstanceResponseBlock}).
 */
public interface Plan {

  /** The root plan node of this instance plan. */
  PlanNode getPlanNode();

  /** Execute the plan and return this instance's response. */
  DataTable execute();
}
