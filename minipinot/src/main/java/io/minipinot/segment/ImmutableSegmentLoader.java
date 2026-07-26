package io.minipinot.segment;

import io.minipinot.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;

/**
 * Loads an on-disk segment directory into a queryable {@link ImmutableSegmentImpl} by memory-mapping
 * its single index file. Mirrors Pinot's {@code ImmutableSegmentLoader}.
 */
public final class ImmutableSegmentLoader {

  private ImmutableSegmentLoader() {
  }

  public static IndexSegment load(File segmentDir)
      throws IOException {
    SegmentDirectory segmentDirectory = SegmentDirectory.open(segmentDir);
    return new ImmutableSegmentImpl(segmentDirectory);
  }
}
