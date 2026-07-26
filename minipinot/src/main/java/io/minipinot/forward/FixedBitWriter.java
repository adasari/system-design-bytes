package io.minipinot.forward;

/**
 * Writes a sequence of unsigned integers each using a fixed number of bits, packed contiguously
 * MSB-first into a byte array. This mirrors the on-disk layout of Pinot's
 * {@code FixedBitSVForwardIndexReader} / {@code FixedBitIntReaderWriter}.
 *
 * <p>Values are appended in document-id order. Bit {@code b} of the stream lives in
 * {@code buffer[b/8]} at bit position {@code 7 - (b%8)} (MSB first), so the packed bytes are
 * portable and endian-independent.
 *
 * <p>The bit-by-bit implementation favors clarity over the word-at-a-time tricks Pinot uses.
 */
public final class FixedBitWriter {
  private final byte[] _buffer;
  private final int _numBits;
  private long _bitPos;

  public FixedBitWriter(int numValues, int numBits) {
    if (numBits < 1 || numBits > 32) {
      throw new IllegalArgumentException("numBits must be in [1,32], got " + numBits);
    }
    _numBits = numBits;
    long totalBits = (long) numValues * numBits;
    _buffer = new byte[(int) ((totalBits + 7) / 8)];
  }

  /** Append one value using {@code numBits} bits. */
  public void putInt(int value) {
    for (int i = _numBits - 1; i >= 0; i--) {
      if (((value >>> i) & 1) != 0) {
        int bytePos = (int) (_bitPos / 8);
        int shift = 7 - (int) (_bitPos % 8);
        _buffer[bytePos] |= (byte) (1 << shift);
      }
      _bitPos++;
    }
  }

  public byte[] toBytes() {
    return _buffer;
  }

  public int sizeInBytes() {
    return _buffer.length;
  }
}
