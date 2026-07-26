package io.minipinot.forward;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * A production-grade, single-valued dictionary-encoded forward index built on Apache Lucene's
 * {@code DirectWriter} / {@code DirectReader}. This is the same packing machinery Lucene uses for
 * doc values, and it is the "real" alternative to MiniPinot's teaching-oriented
 * {@link FixedBitForwardIndex}.
 *
 * <p>Trade-off versus the hand-rolled version: {@code DirectWriter} rounds the bit width up to a
 * byte/word-friendly value (one of 1,2,4,8,12,16,20,24,28,32,40,48,56,64) so reads are aligned and
 * fast, at the cost of storing a few extra bits per value compared to the theoretical minimum.
 * Random access on read is done directly over a memory-mapped {@link ByteBuffer} via
 * {@code ByteBuffersDataInput} (a {@code RandomAccessInput}), with no heap copy of the packed data.
 */
public final class LuceneFixedBitForwardIndex {
  private LuceneFixedBitForwardIndex() {
  }

  /** The (rounded-up, Lucene-supported) bit width used to store dictIds of this cardinality. */
  public static int bitsPerValueFor(int cardinality) {
    return DirectWriter.bitsRequired(Math.max(1, cardinality - 1));
  }

  public static final class Creator {
    private final ByteBuffersDataOutput _out = new ByteBuffersDataOutput();
    private final DirectWriter _writer;
    private final int _bitsPerValue;

    public Creator(int numDocs, int cardinality) {
      _bitsPerValue = bitsPerValueFor(cardinality);
      _writer = DirectWriter.getInstance(_out, numDocs, _bitsPerValue);
    }

    public int getBitsPerValue() {
      return _bitsPerValue;
    }

    public void add(int dictId) {
      try {
        _writer.add(dictId);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public byte[] serialize() {
      try {
        _writer.finish();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return _out.toArrayCopy();
    }
  }

  public static final class Reader implements ForwardIndexReader {
    private final LongValues _values;
    private final int _numDocs;

    public Reader(ByteBuffer buffer, int numDocs, int bitsPerValue) {
      _numDocs = numDocs;
      // DirectReader reads via a little-endian RandomAccessInput over the (mapped) bytes.
      ByteBuffer littleEndian = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
      ByteBuffersDataInput input = new ByteBuffersDataInput(List.of(littleEndian));
      _values = DirectReader.getInstance(input, bitsPerValue);
    }

    @Override
    public int getDictId(int docId) {
      return (int) _values.get(docId);
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }
  }
}
