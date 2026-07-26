package io.minipinot.forward;

/**
 * Selects the implementation used to store a single-valued, dictionary-encoded forward index.
 *
 * <ul>
 *   <li>{@link #HANDCRAFTED} - MiniPinot's own bit-by-bit {@link FixedBitForwardIndex}
 *       (see {@link FixedBitWriter}/{@link FixedBitReader}). Packs at the theoretical minimum
 *       {@code ceil(log2(cardinality))} bits per value; optimized for readability.</li>
 *   <li>{@link #LUCENE_DIRECT} - production-grade packing via Apache Lucene's
 *       {@code DirectWriter}/{@code DirectReader} ({@link LuceneFixedBitForwardIndex}). Rounds the
 *       bit width up to a byte/word-friendly size for fast random access.</li>
 * </ul>
 *
 * The choice is made per segment at build time and recorded in segment metadata so the read path
 * can reconstruct the matching reader.
 */
public enum ForwardIndexEncoding {
  HANDCRAFTED,
  LUCENE_DIRECT
}
