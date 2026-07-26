package io.minipinot.core.query;

import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.parser.CalciteSqlQueryParser;
import io.minipinot.core.plan.Plan;
import io.minipinot.core.plan.maker.InstancePlanMakerImplV2;
import io.minipinot.core.plan.maker.PlanMaker;
import io.minipinot.core.query.aggregation.function.AggregationFunction;
import io.minipinot.core.query.aggregation.function.AggregationFunctionFactory;
import io.minipinot.core.query.reduce.BrokerReduceService;
import io.minipinot.core.query.reduce.ResultTable;
import io.minipinot.core.request.QueryContext;
import io.minipinot.segment.IndexSegment;
import io.minipinot.segment.SegmentContext;
import java.util.ArrayList;
import java.util.List;

/**
 * The end-to-end query entry point, playing the combined role of Pinot's broker + server in a single
 * JVM. It parses SQL with the Calcite-based parser, builds the instance plan (server side), runs it
 * to get one intermediate {@link DataTable} per instance, then reduces those into the final
 * {@link ResultTable} (broker side).
 *
 * <p>This closes the read-path loop the same way Pinot does, with both merge levels visible:
 * {@code SQL -> QueryContext -> makeInstancePlan -> CombinePlanNode/InstanceResponse (per-instance
 * segment merge) -> instance DataTable -> BrokerReduceService (cross-instance merge + finalize) ->
 * ResultTable}. MiniPinot runs one instance, so the broker sees a single-element list — but the
 * layering matches Pinot exactly.
 */
public final class QueryEngine {
  private final List<IndexSegment> _segments;
  private final PlanMaker _planMaker = new InstancePlanMakerImplV2();

  public QueryEngine(List<IndexSegment> segments) {
    _segments = segments;
  }

  public ResultTable query(String sql) {
    QueryContext query = CalciteSqlQueryParser.compile(sql);
    List<AggregationFunction> functions = AggregationFunctionFactory.getAggregationFunctions(query);

    List<SegmentContext> segmentContexts = new ArrayList<>(_segments.size());
    for (IndexSegment segment : _segments) {
      segmentContexts.add(new SegmentContext(segment));
    }
    Plan instancePlan = _planMaker.makeInstancePlan(segmentContexts, query);
    DataTable instanceResponse = instancePlan.execute();

    List<DataTable> dataTables = new ArrayList<>(1);
    dataTables.add(instanceResponse);
    return BrokerReduceService.reduce(dataTables, query, functions);
  }
}
