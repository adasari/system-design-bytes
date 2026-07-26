package io.minipinot.segment;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * The per-segment execution context handed to the plan maker: it wraps the {@link IndexSegment}
 * being queried together with the optional set of documents that are still "queryable" for this
 * query. Mirrors Pinot's {@code org.apache.pinot.segment.spi.SegmentContext}.
 *
 * <p>The {@code queryableDocIdsSnapshot} is how Pinot models upsert / soft-deletes on realtime
 * tables: it is a bitmap of the document ids that are currently valid (e.g. the winning version of
 * each primary key). When present, {@code FilterPlanNode} intersects the query filter with it so
 * superseded/deleted rows are excluded. It is {@code null} for plain immutable segments (MiniPinot's
 * only segment kind today), in which case every document is queryable — but the field is kept so the
 * plan maker matches Pinot exactly and is ready for a future mutable/upsert phase.
 */
public class SegmentContext {
  private final IndexSegment _indexSegment;
  private MutableRoaringBitmap _queryableDocIdsSnapshot;

  public SegmentContext(IndexSegment indexSegment) {
    _indexSegment = indexSegment;
  }

  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  /** Non-null only when the segment restricts which documents are currently valid (upsert/delete). */
  public MutableRoaringBitmap getQueryableDocIdsSnapshot() {
    return _queryableDocIdsSnapshot;
  }

  public void setQueryableDocIdsSnapshot(MutableRoaringBitmap queryableDocIdsSnapshot) {
    _queryableDocIdsSnapshot = queryableDocIdsSnapshot;
  }
}
