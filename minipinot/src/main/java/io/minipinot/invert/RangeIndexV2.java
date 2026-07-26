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
 * Range index version 2 - a Bit-Sliced Index (BSI), MiniPinot's port of Pinot's
 * {@code BitSlicedRangeIndexCreator} / {@code BitSlicedRangeIndexReader} (the current Pinot default).
 *
 * <p><b>Idea.</b> Represent each value (here the dictId) in binary with {@code numBits =
 * ceil(log2(cardinality))} bits, and store one RoaringBitmap per <em>bit position</em>:
 * {@code slice[k]} contains docId {@code d} iff bit {@code k} of {@code value[d]} is 1. Range
 * predicates are then answered <em>exactly</em> by boolean algebra across the slices - there is no
 * "partial" bucket and no forward-index re-check, unlike {@link RangeIndexV1}.
 *
 * <p><b>{@code value <= T} algorithm.</b> Walk bit positions from most- to least-significant,
 * maintaining {@code lt} (docs already strictly less than T) and {@code eq} (docs equal to T on the
 * bits seen so far):
 * <pre>
 *   for k = numBits-1 .. 0:
 *     if bit k of T == 1:  lt |= eq AND NOT slice[k];  eq &= slice[k]
 *     else              :  eq = eq AND NOT slice[k]
 *   result(value <= T) = lt OR eq
 * </pre>
 * A closed range {@code [low, high]} is {@code LE(high) AND NOT LE(low-1)}.
 *
 * <p>Storage is proportional to {@code numBits} bitmaps (~log of the value range) rather than a
 * bitmap per range, and results are self-contained/exact - which is why Pinot promoted BSI to the
 * default range index.
 *
 * <p>Serialized layout:
 * <pre>
 *   int cardinality
 *   int numBits
 *   int numDocs
 *   int[numBits + 1] offsets   // absolute byte offset of each slice bitmap + sentinel
 *   bitmap bytes ...           // one slice per bit position, least-significant first
 * </pre>
 */
public final class RangeIndexV2 {
  private RangeIndexV2() {
  }

  static int numBitsFor(int cardinality) {
    int maxValue = Math.max(0, cardinality - 1);
    return maxValue == 0 ? 1 : 32 - Integer.numberOfLeadingZeros(maxValue);
  }

  public static final class Creator {
    private final int _numDocs;
    private final int _cardinality;
    private final int _numBits;
    private final MutableRoaringBitmap[] _slices;

    public Creator(int numDocs, int cardinality) {
      _numDocs = numDocs;
      _cardinality = cardinality;
      _numBits = numBitsFor(cardinality);
      _slices = new MutableRoaringBitmap[_numBits];
      for (int k = 0; k < _numBits; k++) {
        _slices[k] = new MutableRoaringBitmap();
      }
    }

    public int getNumBits() {
      return _numBits;
    }

    /** Set docId in every slice whose bit is 1 in this value. */
    public void add(int docId, int dictId) {
      for (int k = 0; k < _numBits; k++) {
        if ((dictId >>> k & 1) == 1) {
          _slices[k].add(docId);
        }
      }
    }

    public byte[] serialize() {
      int headerSize = 3 * Integer.BYTES + (_numBits + 1) * Integer.BYTES;
      int[] offsets = new int[_numBits + 1];
      int pos = headerSize;
      for (int k = 0; k < _numBits; k++) {
        offsets[k] = pos;
        pos += _slices[k].serializedSizeInBytes();
      }
      offsets[_numBits] = pos;

      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pos);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(_cardinality);
        dos.writeInt(_numBits);
        dos.writeInt(_numDocs);
        for (int k = 0; k <= _numBits; k++) {
          dos.writeInt(offsets[k]);
        }
        for (int k = 0; k < _numBits; k++) {
          _slices[k].serialize(dos);
        }
        dos.flush();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public static final class Reader {
    private final ByteBuffer _buffer;
    private final int _cardinality;
    private final int _numBits;
    private final int _numDocs;
    private final int _offsetBase;
    private final ImmutableRoaringBitmap[] _slices;
    private final MutableRoaringBitmap _allDocs;

    public Reader(ByteBuffer buffer) {
      _buffer = buffer;
      _cardinality = buffer.getInt(0);
      _numBits = buffer.getInt(Integer.BYTES);
      _numDocs = buffer.getInt(2 * Integer.BYTES);
      _offsetBase = 3 * Integer.BYTES;
      _slices = new ImmutableRoaringBitmap[_numBits];
      for (int k = 0; k < _numBits; k++) {
        _slices[k] = sliceBitmap(k);
      }
      _allDocs = new MutableRoaringBitmap();
      if (_numDocs > 0) {
        _allDocs.add(0L, (long) _numDocs);
      }
    }

    private int offset(int k) {
      return _buffer.getInt(_offsetBase + k * Integer.BYTES);
    }

    private ImmutableRoaringBitmap sliceBitmap(int k) {
      ByteBuffer dup = _buffer.duplicate();
      dup.position(offset(k)).limit(offset(k + 1));
      ByteBuffer slice = dup.slice();
      slice.order(ByteOrder.LITTLE_ENDIAN);
      return new ImmutableRoaringBitmap(slice);
    }

    public int getCardinality() {
      return _cardinality;
    }

    public int getNumBits() {
      return _numBits;
    }

    private MutableRoaringBitmap allDocsCopy() {
      MutableRoaringBitmap copy = new MutableRoaringBitmap();
      copy.or(_allDocs);
      return copy;
    }

    /** Documents whose value is {@code <= threshold} (the core BSI comparison). */
    public MutableRoaringBitmap lessOrEqual(long threshold) {
      if (threshold < 0) {
        return new MutableRoaringBitmap();
      }
      long maxRepresentable = (1L << _numBits) - 1;
      if (threshold >= maxRepresentable) {
        return allDocsCopy();
      }
      MutableRoaringBitmap lt = new MutableRoaringBitmap();   // strictly less than threshold
      MutableRoaringBitmap eq = allDocsCopy();                // equal to threshold on bits seen
      for (int k = _numBits - 1; k >= 0; k--) {
        if ((threshold >>> k & 1) == 1) {
          // A 0 bit here (while equal so far) makes the value strictly smaller.
          lt.or(ImmutableRoaringBitmap.andNot(eq, _slices[k]));
          eq = ImmutableRoaringBitmap.and(eq, _slices[k]);
        } else {
          // Threshold bit is 0: a 1 bit here makes the value larger, so drop those from "equal".
          eq = ImmutableRoaringBitmap.andNot(eq, _slices[k]);
        }
      }
      return ImmutableRoaringBitmap.or(lt, eq);
    }

    /** Exact documents whose dictId lies in the inclusive range {@code [lowDictId, highDictId]}. */
    public MutableRoaringBitmap rangeQuery(int lowDictId, int highDictId) {
      if (highDictId < lowDictId) {
        return new MutableRoaringBitmap();
      }
      MutableRoaringBitmap leHigh = lessOrEqual(highDictId);
      if (lowDictId <= 0) {
        return leHigh;
      }
      MutableRoaringBitmap leLow = lessOrEqual(lowDictId - 1L);
      return ImmutableRoaringBitmap.andNot(leHigh, leLow);
    }
  }
}
