package io.minipinot.core.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minipinot.core.common.Operator;
import io.minipinot.core.datatable.DataTable;
import io.minipinot.core.operator.InstanceResponseOperator;
import io.minipinot.core.operator.combine.BaseCombineOperator;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.query.NonScanBasedAggregationOperator;
import io.minipinot.core.parser.CalciteSqlQueryParser;
import io.minipinot.core.plan.AggregationPlanNode;
import io.minipinot.core.plan.FilterPlanNode;
import io.minipinot.core.plan.InstanceResponsePlanNode;
import io.minipinot.core.plan.maker.InstancePlanMakerImplV2;
import io.minipinot.core.query.reduce.ResultTable;
import io.minipinot.core.request.QueryContext;
import io.minipinot.record.CsvRecordReader;
import io.minipinot.record.GenericRow;
import io.minipinot.segment.ImmutableSegmentLoader;
import io.minipinot.segment.IndexSegment;
import io.minipinot.segment.SegmentContext;
import io.minipinot.spec.Schema;
import io.minipinot.store.SegmentBuildConfig;
import io.minipinot.store.SegmentBuildDriver;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Phase 4 (read path / query engine) integration tests. Splits the sample data into two segments and
 * runs queries through the full {@link QueryEngine} (Calcite parse -&gt; per-segment plan -&gt;
 * instance combine -&gt; broker reduce), comparing against a brute-force oracle over the raw rows.
 */
public class QueryEngineTest {

  private static final File SCHEMA_FILE = new File("src/main/resources/samples/events_schema.json");
  private static final File CSV_FILE = new File("src/main/resources/samples/events.csv");

  private Schema _schema;
  private List<GenericRow> _rows;
  private final List<IndexSegment> _segments = new ArrayList<>();
  private QueryEngine _engine;

  @BeforeEach
  public void setUp()
      throws Exception {
    _schema = Schema.fromJsonFile(SCHEMA_FILE);
    _rows = CsvRecordReader.readAll(CSV_FILE, _schema);

    List<String> lines = Files.readAllLines(CSV_FILE.toPath(), StandardCharsets.UTF_8);
    String header = lines.get(0);
    List<String> data = lines.subList(1, lines.size());
    int mid = data.size() / 2;

    File root = Files.createTempDirectory("mp-query").toFile();
    _segments.add(buildSegment(root, "seg1", header, data.subList(0, mid)));
    _segments.add(buildSegment(root, "seg2", header, data.subList(mid, data.size())));
    _engine = new QueryEngine(_segments);
  }

  @AfterEach
  public void tearDown() {
    for (IndexSegment segment : _segments) {
      segment.close();
    }
    _segments.clear();
  }

  private IndexSegment buildSegment(File root, String name, String header, List<String> dataLines)
      throws Exception {
    File csv = new File(root, name + ".csv");
    List<String> content = new ArrayList<>();
    content.add(header);
    content.addAll(dataLines);
    Files.write(csv.toPath(), content, StandardCharsets.UTF_8);

    SegmentBuildConfig config = new SegmentBuildConfig()
        .withInvertedIndex("country", "browser", "device")
        .withRangeIndex("clicks", "revenue")
        .withBloomFilter("country");
    File outDir = new File(root, name + "_out");
    outDir.mkdirs();
    File segmentDir;
    try (CsvRecordReader reader = new CsvRecordReader(csv, _schema)) {
      segmentDir = new SegmentBuildDriver().build(_schema, reader, name, outDir, config);
    }
    return ImmutableSegmentLoader.load(segmentDir);
  }

  @Test
  public void countStarUsesMetadataPathAndIsCorrect() {
    ResultTable result = _engine.query("SELECT count(*) FROM events");
    assertEquals(List.of("count(*)"), result.getColumnNames());
    assertEquals(1, result.getNumRows());
    assertEquals(10L, result.getRows().get(0)[0]);

    // Verify the plan really chose the no-scan metadata path for a match-all COUNT.
    QueryContext query = CalciteSqlQueryParser.compile("SELECT count(*) FROM events");
    SegmentContext segmentContext = new SegmentContext(_segments.get(0));
    BaseFilterOperator filter = new FilterPlanNode(segmentContext, query).run();
    assertTrue(filter.isResultMatchingAll());
    Operator<DataTable> operator = new AggregationPlanNode(segmentContext, query).run();
    assertInstanceOf(NonScanBasedAggregationOperator.class, operator);
  }

  @Test
  public void minMaxUseMetadataPath() {
    ResultTable result = _engine.query("SELECT min(clicks), max(clicks) FROM events");
    assertEquals(1.0, (double) result.getRows().get(0)[0]);
    assertEquals(7.0, (double) result.getRows().get(0)[1]);

    QueryContext query =
        CalciteSqlQueryParser.compile("SELECT min(clicks), max(clicks) FROM events");
    SegmentContext segmentContext = new SegmentContext(_segments.get(0));
    BaseFilterOperator filter = new FilterPlanNode(segmentContext, query).run();
    assertTrue(filter.isResultMatchingAll());
    Operator<DataTable> operator = new AggregationPlanNode(segmentContext, query).run();
    assertInstanceOf(NonScanBasedAggregationOperator.class, operator);
  }

  @Test
  public void sumWithFilterMatchesBruteForce() {
    ResultTable result = _engine.query("SELECT sum(clicks) FROM events WHERE country = 'US'");
    double expected = 0.0;
    for (GenericRow row : _rows) {
      if ("US".equals(row.getValue("country"))) {
        expected += ((Number) row.getValue("clicks")).doubleValue();
      }
    }
    assertEquals(expected, (double) result.getRows().get(0)[0]);
  }

  @Test
  public void rangeBetweenCountMatchesBruteForce() {
    ResultTable result = _engine.query("SELECT count(*) FROM events WHERE clicks BETWEEN 2 AND 4");
    long expected = _rows.stream()
        .filter(r -> {
          long c = (Long) r.getValue("clicks");
          return c >= 2 && c <= 4;
        }).count();
    assertEquals(expected, result.getRows().get(0)[0]);
  }

  @Test
  public void groupByCountMatchesBruteForce() {
    ResultTable result = _engine.query("SELECT country, count(*) FROM events GROUP BY country");
    Map<String, Long> expected = new HashMap<>();
    for (GenericRow row : _rows) {
      expected.merge((String) row.getValue("country"), 1L, Long::sum);
    }
    Map<String, Long> actual = new HashMap<>();
    for (Object[] r : result.getRows()) {
      actual.put((String) r[0], (Long) r[1]);
    }
    assertEquals(expected, actual);
  }

  @Test
  public void groupByHavingFiltersGroups() {
    ResultTable result = _engine.query(
        "SELECT country, count(*) FROM events GROUP BY country HAVING count(*) > 2");
    Map<String, Long> actual = new HashMap<>();
    for (Object[] r : result.getRows()) {
      actual.put((String) r[0], (Long) r[1]);
    }
    assertEquals(Map.of("US", 4L, "IN", 4L), actual);
  }

  @Test
  public void selectionOrderByDescLimit() {
    ResultTable result =
        _engine.query("SELECT country, clicks FROM events ORDER BY clicks DESC LIMIT 3");
    assertEquals(3, result.getNumRows());
    List<Long> clicks = new ArrayList<>();
    for (Object[] r : result.getRows()) {
      clicks.add((Long) r[1]);
    }
    assertEquals(List.of(7L, 6L, 5L), clicks);
  }

  @Test
  public void andOrFilterMatchesBruteForce() {
    ResultTable result = _engine.query(
        "SELECT count(*) FROM events WHERE country = 'IN' AND clicks >= 5");
    long expected = _rows.stream()
        .filter(r -> "IN".equals(r.getValue("country")) && (Long) r.getValue("clicks") >= 5)
        .count();
    assertEquals(expected, result.getRows().get(0)[0]);
  }

  /**
   * Exercises Pinot's pull-based operator contract: build the instance plan, walk the operator tree
   * (instance-response -&gt; combine -&gt; one segment operator per segment) via
   * {@code getChildOperators()}, then pull the single result block via {@code nextBlock()}.
   */
  @Test
  public void instanceOperatorTreeAndNextBlock() {
    QueryContext query = CalciteSqlQueryParser.compile("SELECT count(*) FROM events");
    List<SegmentContext> segmentContexts = new ArrayList<>();
    for (IndexSegment segment : _segments) {
      segmentContexts.add(new SegmentContext(segment));
    }

    InstanceResponsePlanNode planNode =
        (InstanceResponsePlanNode) new InstancePlanMakerImplV2()
            .makeInstancePlan(segmentContexts, query).getPlanNode();

    // Root of the tree is the instance-response operator.
    Operator<DataTable> instanceOperator = planNode.run();
    assertInstanceOf(InstanceResponseOperator.class, instanceOperator);

    // Its only child is the combine operator...
    List<? extends Operator> combineChildren = instanceOperator.getChildOperators();
    assertEquals(1, combineChildren.size());
    Operator<?> combineOperator = combineChildren.get(0);
    assertInstanceOf(BaseCombineOperator.class, combineOperator);

    // ...which has one leaf segment operator per segment.
    assertEquals(_segments.size(), combineOperator.getChildOperators().size());

    // Pulling the block yields the single instance-level DataTable (data was split into 2 segments,
    // 10 docs total, so the combined COUNT accumulator is 10).
    DataTable block = instanceOperator.nextBlock();
    assertEquals(10L, block.getAggregationIntermediates()[0]);
  }
}
