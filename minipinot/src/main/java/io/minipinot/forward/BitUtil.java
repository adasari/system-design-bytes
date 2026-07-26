package io.minipinot.forward;

/**
 * Helpers for computing the minimal number of bits required to store dictionary ids.
 *
 * <p>Pinot stores single-valued dictionary-encoded columns as a densely bit-packed array of
 * dictionary ids. The width of each element is {@code ceil(log2(cardinality))} bits (at least 1),
 * so a column with cardinality 4 uses 2 bits/doc, cardinality 5 uses 3 bits/doc, and so on. This
 * is the primary space win of dictionary encoding.
 */
public final class BitUtil {
  private BitUtil() {
  }

  /** Number of bits needed to represent dictionary ids {@code 0 .. cardinality-1}. Minimum 1. */
  public static int numBitsForCardinality(int cardinality) {
    if (cardinality <= 1) {
      return 1;
    }
    int maxDictId = cardinality - 1;
    return 32 - Integer.numberOfLeadingZeros(maxDictId);
  }
}
