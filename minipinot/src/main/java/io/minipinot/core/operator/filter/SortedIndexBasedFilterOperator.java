package io.minipinot.core.operator.filter;

import io.minipinot.core.operator.docidsets.BlockDocIdSet;
import io.minipinot.core.operator.docidsets.SortedDocIdSet;
import io.minipinot.forward.SortedForwardIndex;
import java.util.ArrayList;
import java.util.List;

/**
 * The cheapest access path: on a sorted column each dictId occupies a contiguous run of documents,
 * so a set of dictId ranges maps directly to document-id ranges with no scanning or materialization.
 * Used for {@code EQ}/{@code IN}/range predicates when the column is sorted. Mirrors Pinot's
 * {@code SortedIndexBasedFilterOperator}.
 *
 * <p>Each entry of {@code dictIdRanges} is an inclusive {@code [minDictId, maxDictId]} pair; because
 * dictIds are order-preserving they are sorted so the produced document ranges are ascending.
 */
public final class SortedIndexBasedFilterOperator extends BaseFilterOperator {
  private final SortedForwardIndex.Reader _reader;
  private final List<int[]> _dictIdRanges;

  public SortedIndexBasedFilterOperator(SortedForwardIndex.Reader reader, List<int[]> dictIdRanges,
      int numDocs) {
    super(numDocs);
    _reader = reader;
    _dictIdRanges = dictIdRanges;
  }

  @Override
  public BlockDocIdSet getDocIds() {
    List<int[]> sorted = new ArrayList<>(_dictIdRanges);
    sorted.sort((a, b) -> Integer.compare(a[0], b[0]));
    List<int[]> docRanges = new ArrayList<>(sorted.size());
    for (int[] range : sorted) {
      int minDictId = range[0];
      int maxDictId = range[1];
      if (minDictId > maxDictId) {
        continue;
      }
      int startDoc = _reader.getDocIdRange(minDictId)[0];
      int endDoc = _reader.getDocIdRange(maxDictId)[1];
      docRanges.add(new int[]{startDoc, endDoc});
    }
    return new SortedDocIdSet(docRanges);
  }
}
