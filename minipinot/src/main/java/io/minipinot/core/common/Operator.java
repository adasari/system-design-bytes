package io.minipinot.core.common;

import java.util.List;

/**
 * A node in the physical execution tree. Execution is <em>pull-based</em>: a parent asks its child
 * for data by calling {@link #nextBlock()}, exactly like Pinot's
 * {@code org.apache.pinot.core.common.Operator}.
 *
 * <p>For MiniPinot's operators (filter/aggregation/selection/combine/instance-response, all "above
 * the projection phase") {@code nextBlock()} is called <b>once</b> and returns the full non-null
 * result {@link Block}. Pinot additionally streams multiple blocks in the projection phase (docIdSet
 * / projection / transform); MiniPinot inlines projection inside each leaf operator, so it always
 * returns a single block.
 *
 * @param <T> the type of {@link Block} this operator produces
 */
public interface Operator<T extends Block> {

  /**
   * Pull the next {@link Block} from this operator. Called once for MiniPinot's operators and returns
   * a non-null block.
   */
  T nextBlock();

  /** The operators this one pulls from (its children in the execution tree). */
  List<? extends Operator> getChildOperators();

  /** A short human-readable name for this operator, used to render the plan tree (see EXPLAIN). */
  String toExplainString();
}
