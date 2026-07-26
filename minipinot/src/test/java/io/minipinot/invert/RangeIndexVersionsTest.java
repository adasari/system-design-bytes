package io.minipinot.invert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Verifies both range-index versions against a brute-force scan:
 * <ul>
 *   <li>V1 (equi-depth bucketed) - fully-matching docs plus a forward-index re-check of the boundary
 *       ranges must reproduce the exact answer.</li>
 *   <li>V2 (bit-sliced) - {@code rangeQuery} must be exact on its own, no re-check.</li>
 * </ul>
 */
public class RangeIndexVersionsTest {

  private static int[] randomDictIds(int numDocs, int cardinality, long seed) {
    Random random = new Random(seed);
    int[] dictIds = new int[numDocs];
    for (int d = 0; d < numDocs; d++) {
      dictIds[d] = random.nextInt(cardinality);
    }
    return dictIds;
  }

  private static TreeSet<Integer> bruteForce(int[] dictIds, int low, int high) {
    TreeSet<Integer> expected = new TreeSet<>();
    for (int d = 0; d < dictIds.length; d++) {
      if (dictIds[d] >= low && dictIds[d] <= high) {
        expected.add(d);
      }
    }
    return expected;
  }

  private static TreeSet<Integer> toSet(MutableRoaringBitmap bitmap) {
    TreeSet<Integer> set = new TreeSet<>();
    bitmap.forEach((org.roaringbitmap.IntConsumer) set::add);
    return set;
  }

  @Test
  public void v1EquiDepthPlusRecheckIsExact() {
    int numDocs = 2000;
    int cardinality = 50;
    int[] dictIds = randomDictIds(numDocs, cardinality, 1);

    RangeIndexV1.Creator creator = new RangeIndexV1.Creator(numDocs, cardinality);
    for (int d = 0; d < numDocs; d++) {
      creator.add(d, dictIds[d]);
    }
    RangeIndexV1.Reader reader = new RangeIndexV1.Reader(ByteBuffer.wrap(creator.serialize()));

    int[][] ranges = {{0, cardinality - 1}, {10, 20}, {0, 0}, {49, 49}, {5, 5}, {17, 42}};
    for (int[] r : ranges) {
      int low = r[0];
      int high = r[1];
      RangeIndexV1.RangeMatch match = reader.query(low, high);

      // Fully-matching docs must all genuinely be in range (no forward re-check needed for them).
      match._fullyMatching.forEach((org.roaringbitmap.IntConsumer) d ->
          assertTrue(dictIds[d] >= low && dictIds[d] <= high, "full match wrong at doc " + d));

      MutableRoaringBitmap result = new MutableRoaringBitmap();
      result.or(match._fullyMatching);
      // Re-check the boundary ranges against the "forward index" (dictIds array).
      match._partiallyMatching.forEach((org.roaringbitmap.IntConsumer) d -> {
        if (dictIds[d] >= low && dictIds[d] <= high) {
          result.add(d);
        }
      });

      assertEquals(bruteForce(dictIds, low, high), toSet(result),
          "v1 mismatch for [" + low + "," + high + "]");
    }
  }

  @Test
  public void v2BitSlicedIsExactWithoutRecheck() {
    int numDocs = 3000;
    int cardinality = 37; // deliberately not a power of two
    int[] dictIds = randomDictIds(numDocs, cardinality, 2);

    RangeIndexV2.Creator creator = new RangeIndexV2.Creator(numDocs, cardinality);
    for (int d = 0; d < numDocs; d++) {
      creator.add(d, dictIds[d]);
    }
    RangeIndexV2.Reader reader = new RangeIndexV2.Reader(ByteBuffer.wrap(creator.serialize()));

    // Exhaustively test every possible [low, high] sub-range of the dictId domain.
    for (int low = 0; low < cardinality; low++) {
      for (int high = low; high < cardinality; high++) {
        assertEquals(bruteForce(dictIds, low, high), toSet(reader.rangeQuery(low, high)),
            "v2 mismatch for [" + low + "," + high + "]");
      }
    }
    // Empty range and out-of-domain high behave.
    assertTrue(reader.rangeQuery(10, 5).isEmpty());
    assertEquals(bruteForce(dictIds, 0, cardinality - 1),
        toSet(reader.rangeQuery(0, cardinality + 100)));
  }

  @Test
  public void v1AndV2AgreeOnTheSameData() {
    int numDocs = 1500;
    int cardinality = 64;
    int[] dictIds = randomDictIds(numDocs, cardinality, 3);

    RangeIndexV1.Creator v1c = new RangeIndexV1.Creator(numDocs, cardinality);
    RangeIndexV2.Creator v2c = new RangeIndexV2.Creator(numDocs, cardinality);
    for (int d = 0; d < numDocs; d++) {
      v1c.add(d, dictIds[d]);
      v2c.add(d, dictIds[d]);
    }
    RangeIndexV1.Reader v1 = new RangeIndexV1.Reader(ByteBuffer.wrap(v1c.serialize()));
    RangeIndexV2.Reader v2 = new RangeIndexV2.Reader(ByteBuffer.wrap(v2c.serialize()));

    for (int[] r : new int[][]{{8, 40}, {0, 63}, {31, 32}, {0, 0}, {63, 63}}) {
      int low = r[0];
      int high = r[1];
      // Resolve v1 to an exact set via re-check, then compare to v2's self-contained answer.
      RangeIndexV1.RangeMatch match = v1.query(low, high);
      MutableRoaringBitmap v1Exact = new MutableRoaringBitmap();
      v1Exact.or(match._fullyMatching);
      match._partiallyMatching.forEach((org.roaringbitmap.IntConsumer) d -> {
        if (dictIds[d] >= low && dictIds[d] <= high) {
          v1Exact.add(d);
        }
      });
      assertEquals(toSet(v1Exact), toSet(v2.rangeQuery(low, high)),
          "v1 vs v2 disagree for [" + low + "," + high + "]");
    }
  }
}
