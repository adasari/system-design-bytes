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
 * Bitmap inverted index: for every dictionary id it stores the set of document ids that hold that
 * value, as a compressed {@link org.roaringbitmap.buffer.ImmutableRoaringBitmap}. This is the
 * classic value -> docIds acceleration for equality/IN predicates. Mirrors Pinot's
 * {@code OffHeapBitmapInvertedIndexCreator} / {@code BitmapInvertedIndexReader}.
 *
 * <p>Serialized layout (single buffer):
 * <pre>
 *   int cardinality
 *   int[cardinality + 1] offsets   // absolute byte offset of each bitmap, plus end sentinel
 *   bitmap bytes ...               // Roaring portable format, one per dictId
 * </pre>
 * Header ints are big-endian; each bitmap is read back through a little-endian slice (Roaring's
 * on-the-wire requirement) directly from the memory-mapped file with no copy.
 */
public final class BitmapInvertedIndex {
  private BitmapInvertedIndex() {
  }

  public static final class Creator {
    private final MutableRoaringBitmap[] _bitmaps;

    public Creator(int cardinality) {
      _bitmaps = new MutableRoaringBitmap[cardinality];
      for (int i = 0; i < cardinality; i++) {
        _bitmaps[i] = new MutableRoaringBitmap();
      }
    }

    public void add(int docId, int dictId) {
      _bitmaps[dictId].add(docId);
    }

    public byte[] serialize() {
      int cardinality = _bitmaps.length;
      int headerSize = Integer.BYTES + (cardinality + 1) * Integer.BYTES;
      int[] offsets = new int[cardinality + 1];
      int pos = headerSize;
      for (int i = 0; i < cardinality; i++) {
        offsets[i] = pos;
        pos += _bitmaps[i].serializedSizeInBytes();
      }
      offsets[cardinality] = pos;
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pos);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(cardinality);
        for (int i = 0; i <= cardinality; i++) {
          dos.writeInt(offsets[i]);
        }
        for (int i = 0; i < cardinality; i++) {
          _bitmaps[i].serialize(dos);
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

    public Reader(ByteBuffer buffer) {
      _buffer = buffer;
      _cardinality = buffer.getInt(0);
    }

    private int offset(int i) {
      return _buffer.getInt(Integer.BYTES + i * Integer.BYTES);
    }

    public int getCardinality() {
      return _cardinality;
    }

    /** Zero-copy view of the docId bitmap for {@code dictId}. */
    public ImmutableRoaringBitmap getDocIds(int dictId) {
      int start = offset(dictId);
      int end = offset(dictId + 1);
      ByteBuffer dup = _buffer.duplicate();
      dup.position(start).limit(end);
      ByteBuffer slice = dup.slice();
      slice.order(ByteOrder.LITTLE_ENDIAN);
      return new ImmutableRoaringBitmap(slice);
    }
  }
}
