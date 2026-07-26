package io.minipinot.forward;

import java.nio.ByteBuffer;

/**
 * Creates and reads a sorted forward index. When a column's values are already sorted by document
 * id, all documents sharing a dictionary id form one contiguous docId range, so the index only
 * needs to store {@code (minDocId, maxDocId)} per dictionary id. This mirrors Pinot's
 * {@code SingleValueSortedForwardIndexCreator} / {@code SortedIndexReaderImpl}.
 *
 * <p>The same structure doubles as a free inverted index (value -> docId range) and range index,
 * which is why Pinot never builds a separate inverted index for a sorted column.
 */
public final class SortedForwardIndex {
  private SortedForwardIndex() {
  }

  public static final class Creator {
    private final int[] _minDocId;
    private final int[] _maxDocId;

    public Creator(int cardinality) {
      _minDocId = new int[cardinality];
      _maxDocId = new int[cardinality];
      java.util.Arrays.fill(_minDocId, -1);
    }

    /** Feed dictIds in docId order (0,1,2,...). */
    public void add(int docId, int dictId) {
      if (_minDocId[dictId] == -1) {
        _minDocId[dictId] = docId;
      }
      _maxDocId[dictId] = docId;
    }

    /** Serialize as {@code cardinality} pairs of (minDocId, maxDocId) ints. */
    public byte[] serialize() {
      ByteBuffer buffer = ByteBuffer.allocate(_minDocId.length * 2 * Integer.BYTES);
      for (int dictId = 0; dictId < _minDocId.length; dictId++) {
        buffer.putInt(_minDocId[dictId]);
        buffer.putInt(_maxDocId[dictId]);
      }
      return buffer.array();
    }
  }

  public static final class Reader implements ForwardIndexReader {
    private final ByteBuffer _buffer;
    private final int _baseOffset;
    private final int _cardinality;
    private final int _numDocs;

    public Reader(ByteBuffer buffer, int baseOffset, int cardinality, int numDocs) {
      _buffer = buffer;
      _baseOffset = baseOffset;
      _cardinality = cardinality;
      _numDocs = numDocs;
    }

    private int minDocId(int dictId) {
      return _buffer.getInt(_baseOffset + dictId * 2 * Integer.BYTES);
    }

    private int maxDocId(int dictId) {
      return _buffer.getInt(_baseOffset + dictId * 2 * Integer.BYTES + Integer.BYTES);
    }

    @Override
    public int getDictId(int docId) {
      int lo = 0;
      int hi = _cardinality - 1;
      while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (docId < minDocId(mid)) {
          hi = mid - 1;
        } else if (docId > maxDocId(mid)) {
          lo = mid + 1;
        } else {
          return mid;
        }
      }
      throw new IllegalStateException("docId " + docId + " not covered by sorted index");
    }

    /** Inclusive docId range [min,max] for a dictId - the implicit inverted index. */
    public int[] getDocIdRange(int dictId) {
      return new int[]{minDocId(dictId), maxDocId(dictId)};
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }
  }
}
