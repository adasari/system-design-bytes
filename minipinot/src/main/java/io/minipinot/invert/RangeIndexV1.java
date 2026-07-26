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
 * Range index version 1 - a faithful port of Pinot's {@code RangeIndexCreator} (VERSION=1) and
 * {@code RangeIndexReaderImpl}.
 *
 * <p><b>Equi-depth bucketing (the key difference from the simplified {@link RangeIndex}).</b> The
 * sorted dictId domain {@code [0, cardinality)} is split into a fixed <em>number</em> of ranges
 * (default {@link #DEFAULT_NUM_RANGES}) whose boundaries are chosen so that each range holds roughly
 * the same number of <em>documents</em> - exactly Pinot's {@code _numValuesPerRange = numDocs /
 * numRanges}. Range <em>widths</em> in value space therefore vary; the doc count per range is
 * balanced. (The simplified {@link RangeIndex} instead uses equal dictId <em>width</em> buckets.)
 *
 * <p>Each range stores a RoaringBitmap of its documents. A query returns:
 * <ul>
 *   <li>{@code fullyMatching} - documents in ranges entirely inside {@code [low, high]} (exact), and</li>
 *   <li>{@code partiallyMatching} - documents in the (at most two) boundary ranges that the caller
 *       must re-check against the forward index.</li>
 * </ul>
 * This full/partial split is how Pinot's v1 reader stays exact without a bitmap per value.
 *
 * <p>Serialized layout:
 * <pre>
 *   int cardinality
 *   int numRanges
 *   int[numRanges + 1] rangeStartDictId   // boundaries, last == cardinality
 *   int[numRanges + 1] offsets            // absolute byte offset of each range bitmap + sentinel
 *   bitmap bytes ...
 * </pre>
 */
public final class RangeIndexV1 {
  /** Target number of ranges, matching Pinot's {@code RangeIndexCreator.DEFAULT_NUM_RANGES}. */
  public static final int DEFAULT_NUM_RANGES = 20;

  private RangeIndexV1() {
  }

  static int numRangesFor(int cardinality) {
    return Math.max(1, Math.min(cardinality, DEFAULT_NUM_RANGES));
  }

  public static final class Creator {
    private final int _numDocs;
    private final int _cardinality;
    private final int[] _dictIdPerDoc;
    private final int[] _docCountPerDictId;

    public Creator(int numDocs, int cardinality) {
      _numDocs = numDocs;
      _cardinality = cardinality;
      _dictIdPerDoc = new int[numDocs];
      _docCountPerDictId = new int[cardinality];
    }

    /** Feed dictIds in docId order (0,1,2,...). */
    public void add(int docId, int dictId) {
      _dictIdPerDoc[docId] = dictId;
      _docCountPerDictId[dictId]++;
    }

    /** Cut the sorted dictId domain into equi-depth ranges (equal documents per range). */
    private int[] computeRangeStarts() {
      int numRanges = numRangesFor(_cardinality);
      int perRange = Math.max(1, (_numDocs + numRanges - 1) / numRanges);
      int[] starts = new int[numRanges + 1];
      int count = 0;
      starts[count++] = 0;
      int acc = 0;
      for (int dictId = 0; dictId < _cardinality; dictId++) {
        acc += _docCountPerDictId[dictId];
        boolean canAddMore = count < numRanges;
        boolean moreDictIds = dictId + 1 < _cardinality;
        if (acc >= perRange && canAddMore && moreDictIds) {
          starts[count++] = dictId + 1;
          acc = 0;
        }
      }
      starts[count] = _cardinality;
      int[] trimmed = new int[count + 1];
      System.arraycopy(starts, 0, trimmed, 0, count + 1);
      return trimmed;
    }

    private static int rangeOf(int dictId, int[] rangeStart, int numRanges) {
      int lo = 0;
      int hi = numRanges - 1;
      while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (dictId < rangeStart[mid]) {
          hi = mid - 1;
        } else if (dictId >= rangeStart[mid + 1]) {
          lo = mid + 1;
        } else {
          return mid;
        }
      }
      throw new IllegalStateException("dictId " + dictId + " out of range");
    }

    public byte[] serialize() {
      int[] rangeStart = computeRangeStarts();
      int numRanges = rangeStart.length - 1;

      MutableRoaringBitmap[] ranges = new MutableRoaringBitmap[numRanges];
      for (int r = 0; r < numRanges; r++) {
        ranges[r] = new MutableRoaringBitmap();
      }
      for (int docId = 0; docId < _numDocs; docId++) {
        ranges[rangeOf(_dictIdPerDoc[docId], rangeStart, numRanges)].add(docId);
      }

      int headerSize = 2 * Integer.BYTES
          + (numRanges + 1) * Integer.BYTES   // rangeStart
          + (numRanges + 1) * Integer.BYTES;  // offsets
      int[] offsets = new int[numRanges + 1];
      int pos = headerSize;
      for (int r = 0; r < numRanges; r++) {
        offsets[r] = pos;
        pos += ranges[r].serializedSizeInBytes();
      }
      offsets[numRanges] = pos;

      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pos);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(_cardinality);
        dos.writeInt(numRanges);
        for (int r = 0; r <= numRanges; r++) {
          dos.writeInt(rangeStart[r]);
        }
        for (int r = 0; r <= numRanges; r++) {
          dos.writeInt(offsets[r]);
        }
        for (int r = 0; r < numRanges; r++) {
          ranges[r].serialize(dos);
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
    private final int _numRanges;
    private final int _rangeStartBase;
    private final int _offsetBase;

    public Reader(ByteBuffer buffer) {
      _buffer = buffer;
      _cardinality = buffer.getInt(0);
      _numRanges = buffer.getInt(Integer.BYTES);
      _rangeStartBase = 2 * Integer.BYTES;
      _offsetBase = _rangeStartBase + (_numRanges + 1) * Integer.BYTES;
    }

    private int rangeStart(int r) {
      return _buffer.getInt(_rangeStartBase + r * Integer.BYTES);
    }

    private int offset(int r) {
      return _buffer.getInt(_offsetBase + r * Integer.BYTES);
    }

    private ImmutableRoaringBitmap rangeBitmap(int r) {
      ByteBuffer dup = _buffer.duplicate();
      dup.position(offset(r)).limit(offset(r + 1));
      ByteBuffer slice = dup.slice();
      slice.order(ByteOrder.LITTLE_ENDIAN);
      return new ImmutableRoaringBitmap(slice);
    }

    public int getCardinality() {
      return _cardinality;
    }

    /**
     * Documents whose dictId lies in the inclusive range {@code [lowDictId, highDictId]}.
     * Fully-covered ranges are exact; boundary ranges are returned as partial matches.
     */
    public RangeMatch query(int lowDictId, int highDictId) {
      MutableRoaringBitmap full = new MutableRoaringBitmap();
      MutableRoaringBitmap partial = new MutableRoaringBitmap();
      for (int r = 0; r < _numRanges; r++) {
        int rStart = rangeStart(r);
        int rEnd = rangeStart(r + 1) - 1; // inclusive
        if (rEnd < rStart) {
          continue; // empty range
        }
        if (rEnd < lowDictId || rStart > highDictId) {
          continue; // no overlap
        }
        ImmutableRoaringBitmap bitmap = rangeBitmap(r);
        if (rStart >= lowDictId && rEnd <= highDictId) {
          full.or(bitmap); // fully covered
        } else {
          partial.or(bitmap); // boundary range -> re-check needed
        }
      }
      return new RangeMatch(full, partial);
    }
  }
}
