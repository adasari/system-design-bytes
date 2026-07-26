package io.minipinot.invert;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A small Bloom filter over a column's distinct values. It answers "is this value definitely
 * absent?" for equality predicates, letting the query engine skip a segment entirely on a miss.
 * False positives are possible, false negatives are not. Mirrors the role of Pinot's
 * {@code OnHeapGuavaBloomFilterCreator} / bloom filter readers (hand-rolled here to stay
 * dependency free and to expose the mechanics).
 *
 * <p>Uses two 64-bit-derived hashes with the Kirsch-Mitzenmacher scheme
 * ({@code h_i = h1 + i * h2}) to synthesize {@code k} hash functions from one FNV-1a hash.
 *
 * <p>Serialized layout: {@code int numBits, int numHashes, int numWords, long[numWords] bits}.
 */
public final class BloomFilter {
  private static final double DEFAULT_FPP = 0.03;

  private BloomFilter() {
  }

  public static final class Creator {
    private final int _numBits;
    private final int _numHashes;
    private final long[] _words;

    public Creator(int expectedInsertions) {
      int n = Math.max(1, expectedInsertions);
      _numBits = optimalNumBits(n, DEFAULT_FPP);
      _numHashes = optimalNumHashes(n, _numBits);
      _words = new long[(_numBits + 63) / 64];
    }

    public void add(Object value) {
      long hash = fnv1a64(toBytes(value));
      long h1 = hash & 0xffffffffL;
      long h2 = hash >>> 32;
      for (int i = 0; i < _numHashes; i++) {
        int bitIndex = (int) Long.remainderUnsigned(h1 + (long) i * h2, _numBits);
        _words[bitIndex / 64] |= 1L << (bitIndex % 64);
      }
    }

    public byte[] serialize() {
      ByteBuffer buffer =
          ByteBuffer.allocate(3 * Integer.BYTES + _words.length * Long.BYTES);
      buffer.putInt(_numBits);
      buffer.putInt(_numHashes);
      buffer.putInt(_words.length);
      for (long word : _words) {
        buffer.putLong(word);
      }
      return buffer.array();
    }
  }

  public static final class Reader {
    private final ByteBuffer _buffer;
    private final int _numBits;
    private final int _numHashes;
    private final int _wordsBase;

    public Reader(ByteBuffer buffer) {
      _buffer = buffer;
      _numBits = buffer.getInt(0);
      _numHashes = buffer.getInt(Integer.BYTES);
      _wordsBase = 3 * Integer.BYTES;
    }

    /** False means the value is definitely absent; true means it may be present. */
    public boolean mightContain(Object value) {
      long hash = fnv1a64(toBytes(value));
      long h1 = hash & 0xffffffffL;
      long h2 = hash >>> 32;
      for (int i = 0; i < _numHashes; i++) {
        int bitIndex = (int) Long.remainderUnsigned(h1 + (long) i * h2, _numBits);
        long word = _buffer.getLong(_wordsBase + (bitIndex / 64) * Long.BYTES);
        if ((word & (1L << (bitIndex % 64))) == 0) {
          return false;
        }
      }
      return true;
    }
  }

  // ---- helpers -----------------------------------------------------------

  static int optimalNumBits(long n, double p) {
    return (int) Math.max(1, Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2))));
  }

  static int optimalNumHashes(long n, int m) {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  static long fnv1a64(byte[] data) {
    long hash = 0xcbf29ce484222325L;
    for (byte b : data) {
      hash ^= (b & 0xff);
      hash *= 0x100000001b3L;
    }
    return hash;
  }

  /** Deterministic, type-stable byte encoding so build and query hash identically. */
  static byte[] toBytes(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }
    if (value instanceof Integer) {
      return ByteBuffer.allocate(Integer.BYTES).putInt((Integer) value).array();
    }
    if (value instanceof Long) {
      return ByteBuffer.allocate(Long.BYTES).putLong((Long) value).array();
    }
    if (value instanceof Float) {
      return ByteBuffer.allocate(Integer.BYTES).putInt(Float.floatToIntBits((Float) value)).array();
    }
    if (value instanceof Double) {
      return ByteBuffer.allocate(Long.BYTES).putLong(Double.doubleToLongBits((Double) value)).array();
    }
    return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
  }
}
