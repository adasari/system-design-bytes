package io.minipinot.core.query.utils;

import io.minipinot.core.request.OrderByExpressionContext;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;

/**
 * Value comparison helpers shared by the ordered-selection operator (per-segment top-N) and the
 * broker reduce (global sort). Keeping a single comparison rule guarantees the local and global
 * orderings agree. Mirrors the role of Pinot's selection/order-by comparators.
 */
public final class ValueComparators {

  private ValueComparators() {
  }

  /**
   * Total order over column values: numbers compared numerically, {@code byte[]} lexicographically,
   * everything else via its natural {@link Comparable} order (e.g. strings). {@code null}s sort last.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int compareValues(Object a, Object b) {
    if (a == null || b == null) {
      return a == null ? (b == null ? 0 : 1) : -1;
    }
    if (a instanceof Number && b instanceof Number) {
      return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
    }
    if (a instanceof byte[] && b instanceof byte[]) {
      return new String((byte[]) a, StandardCharsets.ISO_8859_1)
          .compareTo(new String((byte[]) b, StandardCharsets.ISO_8859_1));
    }
    return ((Comparable) a).compareTo(b);
  }

  /**
   * A comparator over order-by <em>key arrays</em> (one entry per order-by expression, in order),
   * honoring each key's ASC/DESC direction.
   */
  public static Comparator<Object[]> orderByComparator(List<OrderByExpressionContext> orderBys) {
    boolean[] asc = new boolean[orderBys.size()];
    for (int i = 0; i < orderBys.size(); i++) {
      asc[i] = orderBys.get(i).isAsc();
    }
    return (a, b) -> {
      for (int i = 0; i < asc.length; i++) {
        int cmp = compareValues(a[i], b[i]);
        if (cmp != 0) {
          return asc[i] ? cmp : -cmp;
        }
      }
      return 0;
    };
  }
}
