package io.minipinot.core.plan;

import io.minipinot.core.operator.filter.AndFilterOperator;
import io.minipinot.core.operator.filter.BaseFilterOperator;
import io.minipinot.core.operator.filter.BitmapBasedFilterOperator;
import io.minipinot.core.operator.filter.EmptyFilterOperator;
import io.minipinot.core.operator.filter.MatchAllFilterOperator;
import io.minipinot.core.operator.filter.NotFilterOperator;
import io.minipinot.core.operator.filter.OrFilterOperator;
import io.minipinot.core.operator.filter.RangeIndexBasedFilterOperator;
import io.minipinot.core.operator.filter.ScanBasedFilterOperator;
import io.minipinot.core.operator.filter.SortedIndexBasedFilterOperator;
import io.minipinot.core.request.FilterContext;
import io.minipinot.core.request.QueryContext;
import io.minipinot.core.request.predicate.EqPredicate;
import io.minipinot.core.request.predicate.InPredicate;
import io.minipinot.core.request.predicate.NotEqPredicate;
import io.minipinot.core.request.predicate.Predicate;
import io.minipinot.core.request.predicate.RangePredicate;
import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.SortedForwardIndex;
import io.minipinot.segment.DataSource;
import io.minipinot.segment.IndexSegment;
import io.minipinot.segment.SegmentContext;
import io.minipinot.spec.DataType;
import io.minipinot.store.ColumnMetadata;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntPredicate;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Turns a {@link FilterContext} tree into an executable {@link BaseFilterOperator} tree for one
 * segment, choosing the cheapest access path per leaf exactly as Pinot's {@code FilterPlanNode} does:
 * <ol>
 *   <li>column is sorted -&gt; {@link SortedIndexBasedFilterOperator} (dictId ranges -&gt; doc ranges),</li>
 *   <li>an inverted index exists and the predicate is {@code EQ}/{@code IN} -&gt;
 *       {@link BitmapBasedFilterOperator},</li>
 *   <li>a range index exists and the predicate is a range -&gt; {@link RangeIndexBasedFilterOperator},</li>
 *   <li>otherwise a lazy {@link ScanBasedFilterOperator}.</li>
 * </ol>
 *
 * <p>Every dictionary predicate is first reduced to bounds/membership over the column's
 * order-preserving dictionary ids (via binary search), which is what lets the same predicate feed a
 * sorted range, a bitmap union, a range lookup or a scan uniformly. An {@code EQ} whose value the
 * bloom filter rejects short-circuits to {@link EmptyFilterOperator} with no index access at all.
 *
 * <p>When the {@link SegmentContext} carries a queryable-docs snapshot (upsert/soft-delete), the
 * constructed filter is intersected with it — mirroring Pinot's {@code FilterPlanNode#run()}.
 */
public final class FilterPlanNode {
  private final SegmentContext _segmentContext;
  private final IndexSegment _segment;
  private final FilterContext _filter;

  public FilterPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    this(segmentContext, queryContext.getFilter());
  }

  public FilterPlanNode(SegmentContext segmentContext, FilterContext filter) {
    _segmentContext = segmentContext;
    _segment = segmentContext.getIndexSegment();
    _filter = filter;
  }

  public BaseFilterOperator run() {
    int numDocs = _segment.getTotalDocCount();
    MutableRoaringBitmap queryableDocIdsSnapshot = _segmentContext.getQueryableDocIdsSnapshot();
    if (_filter != null) {
      BaseFilterOperator filterOperator = build(_filter);
      if (queryableDocIdsSnapshot != null) {
        BaseFilterOperator validDocFilter =
            new BitmapBasedFilterOperator(queryableDocIdsSnapshot, numDocs);
        return new AndFilterOperator(List.of(filterOperator, validDocFilter), numDocs);
      }
      return filterOperator;
    }
    if (queryableDocIdsSnapshot != null) {
      return new BitmapBasedFilterOperator(queryableDocIdsSnapshot, numDocs);
    }
    return new MatchAllFilterOperator(numDocs);
  }

  private BaseFilterOperator build(FilterContext filter) {
    int numDocs = _segment.getTotalDocCount();
    switch (filter.getType()) {
      case AND: {
        List<BaseFilterOperator> children = new ArrayList<>();
        for (FilterContext child : filter.getChildren()) {
          children.add(build(child));
        }
        return new AndFilterOperator(children, numDocs);
      }
      case OR: {
        List<BaseFilterOperator> children = new ArrayList<>();
        for (FilterContext child : filter.getChildren()) {
          children.add(build(child));
        }
        return new OrFilterOperator(children, numDocs);
      }
      case NOT:
        return new NotFilterOperator(build(filter.getChildren().get(0)), numDocs);
      case PREDICATE:
        return buildLeaf(filter.getPredicate());
      default:
        throw new IllegalStateException("Unhandled filter type: " + filter.getType());
    }
  }

  private BaseFilterOperator buildLeaf(Predicate predicate) {
    String column = predicate.getLhs().getIdentifier();
    DataSource dataSource = _segment.getDataSource(column);
    ColumnMetadata metadata = dataSource.getDataSourceMetadata();
    DataType dataType = metadata.getDataType();
    SortedDictionary dictionary = dataSource.getDictionary();
    int numDocs = _segment.getTotalDocCount();
    int cardinality = metadata.getCardinality();

    switch (predicate.getType()) {
      case EQ: {
        Object value = dataType.parse(((EqPredicate) predicate).getValue());
        if (dataSource.getBloomFilter() != null && !dataSource.getBloomFilter().mightContain(value)) {
          return new EmptyFilterOperator(numDocs);
        }
        int dictId = dictionary.indexOf(value);
        if (dictId < 0) {
          return new EmptyFilterOperator(numDocs);
        }
        if (metadata.isSorted()) {
          return sorted(dataSource, List.of(new int[]{dictId, dictId}), numDocs);
        }
        if (dataSource.getInvertedIndex() != null) {
          return new BitmapBasedFilterOperator(dataSource.getInvertedIndex(), new int[]{dictId},
              numDocs);
        }
        return new ScanBasedFilterOperator(dataSource.getForwardIndex(), d -> d == dictId, numDocs);
      }
      case NOT_EQ: {
        Object value = dataType.parse(((NotEqPredicate) predicate).getValue());
        int dictId = dictionary.indexOf(value);
        if (metadata.isSorted()) {
          List<int[]> ranges = new ArrayList<>();
          if (dictId < 0) {
            ranges.add(new int[]{0, cardinality - 1});
          } else {
            if (dictId > 0) {
              ranges.add(new int[]{0, dictId - 1});
            }
            if (dictId < cardinality - 1) {
              ranges.add(new int[]{dictId + 1, cardinality - 1});
            }
          }
          if (ranges.isEmpty()) {
            return new EmptyFilterOperator(numDocs);
          }
          return sorted(dataSource, ranges, numDocs);
        }
        return new ScanBasedFilterOperator(dataSource.getForwardIndex(), d -> d != dictId, numDocs);
      }
      case IN: {
        List<Integer> dictIds = new ArrayList<>();
        for (String raw : ((InPredicate) predicate).getValues()) {
          int dictId = dictionary.indexOf(dataType.parse(raw));
          if (dictId >= 0) {
            dictIds.add(dictId);
          }
        }
        if (dictIds.isEmpty()) {
          return new EmptyFilterOperator(numDocs);
        }
        if (metadata.isSorted()) {
          List<int[]> ranges = new ArrayList<>();
          for (int dictId : dictIds) {
            ranges.add(new int[]{dictId, dictId});
          }
          return sorted(dataSource, ranges, numDocs);
        }
        if (dataSource.getInvertedIndex() != null) {
          int[] array = dictIds.stream().mapToInt(Integer::intValue).toArray();
          return new BitmapBasedFilterOperator(dataSource.getInvertedIndex(), array, numDocs);
        }
        Set<Integer> set = new HashSet<>(dictIds);
        IntPredicate matcher = set::contains;
        return new ScanBasedFilterOperator(dataSource.getForwardIndex(), matcher, numDocs);
      }
      case RANGE: {
        RangePredicate range = (RangePredicate) predicate;
        int lowDictId = lowDictId(range, dictionary, dataType);
        int highDictId = highDictId(range, dictionary, dataType, cardinality);
        if (lowDictId > highDictId) {
          return new EmptyFilterOperator(numDocs);
        }
        if (metadata.isSorted()) {
          return sorted(dataSource, List.of(new int[]{lowDictId, highDictId}), numDocs);
        }
        if (dataSource.getRangeIndex() != null) {
          return new RangeIndexBasedFilterOperator(dataSource.getRangeIndex(),
              dataSource.getForwardIndex(), lowDictId, highDictId, numDocs);
        }
        int low = lowDictId;
        int high = highDictId;
        return new ScanBasedFilterOperator(dataSource.getForwardIndex(),
            d -> d >= low && d <= high, numDocs);
      }
      default:
        throw new IllegalStateException("Unhandled predicate type: " + predicate.getType());
    }
  }

  private static BaseFilterOperator sorted(DataSource dataSource, List<int[]> dictIdRanges,
      int numDocs) {
    return new SortedIndexBasedFilterOperator((SortedForwardIndex.Reader) dataSource.getForwardIndex(),
        dictIdRanges, numDocs);
  }

  /** First dictId whose value satisfies the lower bound (inclusive -&gt; firstGE, exclusive -&gt; firstGT). */
  private static int lowDictId(RangePredicate range, SortedDictionary dictionary, DataType dataType) {
    if (range.isLowerUnbounded()) {
      return 0;
    }
    int idx = dictionary.indexOf(dataType.parse(range.getLowerBound()));
    if (range.isLowerInclusive()) {
      return idx >= 0 ? idx : -(idx) - 1;
    }
    return idx >= 0 ? idx + 1 : -(idx) - 1;
  }

  /** Last dictId whose value satisfies the upper bound (inclusive -&gt; lastLE, exclusive -&gt; lastLT). */
  private static int highDictId(RangePredicate range, SortedDictionary dictionary, DataType dataType,
      int cardinality) {
    if (range.isUpperUnbounded()) {
      return cardinality - 1;
    }
    int idx = dictionary.indexOf(dataType.parse(range.getUpperBound()));
    if (range.isUpperInclusive()) {
      return idx >= 0 ? idx : -(idx) - 2;
    }
    return idx >= 0 ? idx - 1 : -(idx) - 2;
  }
}
