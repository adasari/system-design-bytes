package io.minipinot.record;

import java.io.Closeable;

/**
 * Streams input rows as {@link GenericRow}s. Mirrors
 * {@code org.apache.pinot.spi.data.readers.RecordReader}: the segment creation driver pulls
 * rows one at a time, decoupling the on-disk input format (CSV/JSON/Avro) from segment building.
 */
public interface RecordReader extends Closeable {

  boolean hasNext();

  /** Reads the next row. May reuse the passed-in reuse instance for efficiency. */
  GenericRow next(GenericRow reuse);

  /** Rewinds to the beginning so the driver can make a second pass (stats then index). */
  void rewind();
}
