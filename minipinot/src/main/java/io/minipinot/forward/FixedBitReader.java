package io.minipinot.forward;

import java.nio.ByteBuffer;

/**
 * Random-access reader over a fixed-bit-packed integer array produced by {@link FixedBitWriter}.
 * Reads directly from a {@link ByteBuffer} (which in Phase 2 will be a memory-mapped slice of the
 * segment's single index file), so no per-value deserialization onto the heap is required.
 *
 * <p>Equivalent role to Pinot's {@code FixedBitSVForwardIndexReader}: given a docId it returns the
 * packed dictionary id in O(numBits).
 */
public final class FixedBitReader {
  private final ByteBuffer _buffer;
  private final int _baseOffset;
  private final int _numBits;

  public FixedBitReader(ByteBuffer buffer, int baseOffset, int numBits) {
    _buffer = buffer;
    _baseOffset = baseOffset;
    _numBits = numBits;
  }

  public FixedBitReader(ByteBuffer buffer, int numBits) {
    this(buffer, 0, numBits);
  }

  /** Return the unsigned integer stored at logical index {@code docId}. */
  public int getInt(int docId) {
    long startBit = (long) docId * _numBits;
    int value = 0;
    for (int i = 0; i < _numBits; i++) {
      long bitPos = startBit + i;
      int bytePos = _baseOffset + (int) (bitPos / 8);
      int shift = 7 - (int) (bitPos % 8);
      int bit = (_buffer.get(bytePos) >>> shift) & 1;
      value = (value << 1) | bit;
    }
    return value;
  }
}
