package io.minipinot.forward;

import java.nio.ByteBuffer;

/**
 * Creates and reads a bit-packed, single-valued, dictionary-encoded forward index. Dictionary ids
 * are appended in document-id order and packed at {@code numBits} bits each. This is the default
 * forward index for an unsorted dictionary-encoded column, mirroring Pinot's
 * {@code SingleValueUnsortedForwardIndexCreator} + {@code FixedBitSVForwardIndexReader}.
 */
public final class FixedBitForwardIndex {
  private FixedBitForwardIndex() {
  }

  /** Write-side builder: append dictIds in docId order, then {@link Creator#serialize()}. */
  public static final class Creator {
    private final FixedBitWriter _writer;
    private final int _numBits;

    public Creator(int numDocs, int cardinality) {
      _numBits = BitUtil.numBitsForCardinality(cardinality);
      _writer = new FixedBitWriter(numDocs, _numBits);
    }

    public void add(int dictId) {
      _writer.putInt(dictId);
    }

    public int getNumBits() {
      return _numBits;
    }

    public byte[] serialize() {
      return _writer.toBytes();
    }
  }

  /** Read-side view over a (memory-mapped) buffer slice. */
  public static final class Reader implements ForwardIndexReader {
    private final FixedBitReader _reader;
    private final int _numDocs;

    public Reader(ByteBuffer buffer, int baseOffset, int numDocs, int numBits) {
      _reader = new FixedBitReader(buffer, baseOffset, numBits);
      _numDocs = numDocs;
    }

    @Override
    public int getDictId(int docId) {
      return _reader.getInt(docId);
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }
  }
}
