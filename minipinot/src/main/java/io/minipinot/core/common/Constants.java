package io.minipinot.core.common;

/** Shared query-execution constants. Mirrors Pinot's {@code org.apache.pinot.core.common.Constants}. */
public final class Constants {
  /** Returned by a {@code BlockDocIdIterator} once it is exhausted. */
  public static final int EOF = -1;

  private Constants() {
  }
}
