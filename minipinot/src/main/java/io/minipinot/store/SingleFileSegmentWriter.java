package io.minipinot.store;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Writes a segment in the "single file" format (Pinot's V3 layout): every per-column index buffer
 * is concatenated into one file {@code columns.psf}, and an {@code index_map} records the byte
 * offset and length of each (column, indexType) entry. Mirrors Pinot's
 * {@code SingleFileIndexDirectory} write side.
 *
 * <p>Layout of {@code columns.psf}: for each entry we write an 8-byte MAGIC marker (for validation)
 * immediately followed by the raw index bytes. The {@code index_map} startOffset points at the
 * data bytes (just after the marker).
 */
public final class SingleFileSegmentWriter {
  public static final String INDEX_FILE_NAME = "columns.psf";
  public static final String INDEX_MAP_FILE_NAME = "index_map";
  static final long MAGIC_MARKER = 0xdeadbeefdeafbeadL;
  static final int MAGIC_MARKER_SIZE = 8;

  private final Map<IndexKey, byte[]> _entries = new LinkedHashMap<>();

  /** Register a buffer to be persisted under (column, indexType). */
  public SingleFileSegmentWriter add(IndexKey key, byte[] bytes) {
    _entries.put(key, bytes);
    return this;
  }

  public SingleFileSegmentWriter add(String column, IndexType indexType, byte[] bytes) {
    return add(new IndexKey(column, indexType), bytes);
  }

  /** Persist columns.psf + index_map + metadata.properties into {@code segmentDir}. */
  public void write(File segmentDir, SegmentMetadata metadata)
      throws IOException {
    if (!segmentDir.exists() && !segmentDir.mkdirs()) {
      throw new IOException("Could not create segment dir: " + segmentDir);
    }

    Properties indexMap = new Properties();
    File psf = new File(segmentDir, INDEX_FILE_NAME);
    try (DataOutputStream out =
        new DataOutputStream(Files.newOutputStream(psf.toPath()))) {
      long position = 0;
      for (Map.Entry<IndexKey, byte[]> entry : _entries.entrySet()) {
        out.writeLong(MAGIC_MARKER);
        position += MAGIC_MARKER_SIZE;
        byte[] bytes = entry.getValue();
        out.write(bytes);
        String prefix = entry.getKey().toPropertyPrefix();
        indexMap.setProperty(prefix + ".startOffset", Long.toString(position));
        indexMap.setProperty(prefix + ".size", Long.toString(bytes.length));
        position += bytes.length;
      }
    }

    try (OutputStream out =
        Files.newOutputStream(new File(segmentDir, INDEX_MAP_FILE_NAME).toPath())) {
      indexMap.store(out, "MiniPinot index_map: <column>.<indexType>.{startOffset,size}");
    }

    metadata.write(segmentDir);
  }
}
