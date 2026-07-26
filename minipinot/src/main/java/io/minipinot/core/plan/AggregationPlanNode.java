package io.minipinot.core.plan;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.query.AggregationOperator;
import io.minipinot.core.operator.query.NonScanBasedAggregationOperator;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.aggregation.function.AggregationFunctionFactory;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.IndexSegment;
import io.minipinot.segment.SegmentContext;
import java.util.List;

/**
 * Plans a non-grouped aggregation over one segment. It builds the segment's filter operator (via
 * {@link FilterPlanNode}), then picks the metadata-only path ({@link NonScanBasedAggregationOperator})
 * when the whole segment is matched <em>and</em> every function can be answered from metadata;
 * otherwise it falls back to the scan-based {@link AggregationOperator}. Mirrors Pinot's
 * {@code AggregationPlanNode}, which switches to {@code NonScanBasedAggregationOperator} under the
 * same condition and likewise constructs its own {@code FilterPlanNode} inside {@code run()}.
 */
public final class AggregationPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;

  public AggregationPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
  }

  @Override
  public Operator<DataTable> run() {
    List<AggregationFunction> functions =
        AggregationFunctionFactory.getAggregationFunctions(_queryContext);
    BaseFilterOperator filterOperator = new FilterPlanNode(_segmentContext, _queryContext).run();
    if (filterOperator.isResultMatchingAll() && allMetadataBased(functions)) {
      return new NonScanBasedAggregationOperator(functions, _indexSegment);
    }
    return new AggregationOperator(functions, filterOperator, _indexSegment);
  }

  private static boolean allMetadataBased(List<AggregationFunction> functions) {
    for (AggregationFunction function : functions) {
      if (!function.isMetadataBased()) {
        return false;
      }
    }
    return true;
  }
}
