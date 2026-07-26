package io.minipinot.invert;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Bucketed range index over the (sorted) dictionary-id space. Instead of one bitmap per value, the
 * dictId range {@code [0, cardinality)} is partitioned into contiguous buckets, each holding a
 * bitmap of its documents. Because the dictionary is sorted, a value range maps to a dictId range,
 * so range predicates touch only a few bucket bitmaps. Mirrors the intent of Pinot's
 * {@code RangeIndexCreator} / {@code RangeIndexReader}.
 *
 * <p>A query returns two bitmaps: documents that are <em>fully</em> inside the range (buckets
 * entirely covered) and documents that are <em>partially</em> matching (boundary buckets that the
 * caller must re-check against the forward index). This full/partial split is exactly how Pinot's
 * range reader avoids per-value bitmaps while staying exact.
 *
 * <p>Serialized layout:
 * <pre>
 *   int cardinality
 *   int numBuckets
 *   int[numBuckets + 1] bucketStartDictId   // boundaries, last == cardinality
 *   int[numBuckets + 1] offsets             // absolute byte offset of each bucket bitmap + sentinel
 *   bitmap bytes ...
 * </pre>
 */
public final class RangeIndex {
  /** Default cap on the number of buckets; keeps boundary re-scans small. */
  public static final int DEFAULT_MAX_BUCKETS = 16;

  private RangeIndex() {
  }

  static int numBucketsFor(int cardinality) {
    return Math.max(1, Math.min(cardinality, DEFAULT_MAX_BUCKETS));
  }

  static int bucketOf(int dictId, int cardinality, int numBuckets) {
    return (int) ((long) dictId * numBuckets / cardinality);
  }

  public static final class Creator {
    private final int _cardinality;
    private final int _numBuckets;
    private final MutableRoaringBitmap[] _buckets;

    public Creator(int cardinality) {
      _cardinality = cardinality;
      _numBuckets = numBucketsFor(cardinality);
      _buckets = new MutableRoaringBitmap[_numBuckets];
      for (int i = 0; i < _numBuckets; i++) {
        _buckets[i] = new MutableRoaringBitmap();
      }
    }

    public void add(int docId, int dictId) {
      _buckets[bucketOf(dictId, _cardinality, _numBuckets)].add(docId);
    }

    public byte[] serialize() {
      int[] bucketStart = new int[_numBuckets + 1];
      for (int b = 0; b <= _numBuckets; b++) {
        // Smallest dictId that maps to bucket b (inverse of bucketOf).
        bucketStart[b] = (int) Math.ceil((double) b * _cardinality / _numBuckets);
      }
      bucketStart[_numBuckets] = _cardinality;

      int headerSize = 2 * Integer.BYTES
          + (_numBuckets + 1) * Integer.BYTES   // bucketStart
          + (_numBuckets + 1) * Integer.BYTES;  // offsets
      int[] offsets = new int[_numBuckets + 1];
      int pos = headerSize;
      for (int b = 0; b < _numBuckets; b++) {
        offsets[b] = pos;
        pos += _buckets[b].serializedSizeInBytes();
      }
      offsets[_numBuckets] = pos;

      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pos);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(_cardinality);
        dos.writeInt(_numBuckets);
        for (int b = 0; b <= _numBuckets; b++) {
          dos.writeInt(bucketStart[b]);
        }
        for (int b = 0; b <= _numBuckets; b++) {
          dos.writeInt(offsets[b]);
        }
        for (int b = 0; b < _numBuckets; b++) {
          _buckets[b].serialize(dos);
        }
        dos.flush();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /** Result of a range lookup: exact matches plus boundary docs needing a forward-index re-check. */
  public static final class RangeMatch {
    public final MutableRoaringBitmap _fullyMatching;
    public final MutableRoaringBitmap _partiallyMatching;

    RangeMatch(MutableRoaringBitmap fullyMatching, MutableRoaringBitmap partiallyMatching) {
      _fullyMatching = fullyMatching;
      _partiallyMatching = partiallyMatching;
    }
  }

  public static final class Reader {
    private final ByteBuffer _buffer;
    private final int _cardinality;
    private final int _numBuckets;
    private final int _bucketStartBase;
    private final int _offsetBase;

    public Reader(ByteBuffer buffer) {
      _buffer = buffer;
      _cardinality = buffer.getInt(0);
      _numBuckets = buffer.getInt(Integer.BYTES);
      _bucketStartBase = 2 * Integer.BYTES;
      _offsetBase = _bucketStartBase + (_numBuckets + 1) * Integer.BYTES;
    }

    private int bucketStart(int b) {
      return _buffer.getInt(_bucketStartBase + b * Integer.BYTES);
    }

    private int offset(int b) {
      return _buffer.getInt(_offsetBase + b * Integer.BYTES);
    }

    private ImmutableRoaringBitmap bucketBitmap(int b) {
      int start = offset(b);
      int end = offset(b + 1);
      ByteBuffer dup = _buffer.duplicate();
      dup.position(start).limit(end);
      ByteBuffer slice = dup.slice();
      slice.order(ByteOrder.LITTLE_ENDIAN);
      return new ImmutableRoaringBitmap(slice);
    }

    public int getCardinality() {
      return _cardinality;
    }

    /**
     * Return documents whose dictId lies in the inclusive range {@code [lowDictId, highDictId]}.
     * Fully-covered buckets are exact; boundary buckets are returned as partial matches.
     */
    public RangeMatch query(int lowDictId, int highDictId) {
      MutableRoaringBitmap full = new MutableRoaringBitmap();
      MutableRoaringBitmap partial = new MutableRoaringBitmap();
      for (int b = 0; b < _numBuckets; b++) {
        int bStart = bucketStart(b);
        int bEnd = bucketStart(b + 1) - 1; // inclusive
        if (bEnd < bStart) {
          continue; // empty bucket
        }
        if (bEnd < lowDictId || bStart > highDictId) {
          continue; // no overlap
        }
        ImmutableRoaringBitmap bitmap = bucketBitmap(b);
        if (bStart >= lowDictId && bEnd <= highDictId) {
          full.or(bitmap); // fully covered
        } else {
          partial.or(bitmap); // boundary bucket -> re-check needed
        }
      }
      return new RangeMatch(full, partial);
    }
  }
}
