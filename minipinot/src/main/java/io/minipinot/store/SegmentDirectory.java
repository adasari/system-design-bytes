package io.minipinot.store;

import io.minipinot.buffer.Mmap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Read side of the single-file segment format. On open it loads {@code metadata.properties}, parses
 * {@code index_map}, and memory-maps {@code columns.psf}. {@link #getBuffer} hands out a zero-based
 * {@link ByteBuffer} slice for any (column, indexType) so index readers can address bytes directly
 * from the mapped file. Mirrors Pinot's {@code SingleFileIndexDirectory} read side.
 */
public final class SegmentDirectory implements AutoCloseable {
  private final File _segmentDir;
  private final SegmentMetadata _metadata;
  private final MappedByteBuffer _indexBuffer;
  private final Map<IndexKey, long[]> _offsets = new HashMap<>(); // key -> [startOffset, size]

  private SegmentDirectory(File segmentDir, SegmentMetadata metadata, MappedByteBuffer indexBuffer,
      Map<IndexKey, long[]> offsets) {
    _segmentDir = segmentDir;
    _metadata = metadata;
    _indexBuffer = indexBuffer;
    _offsets.putAll(offsets);
  }

  public static SegmentDirectory open(File segmentDir)
      throws IOException {
    SegmentMetadata metadata = SegmentMetadata.read(segmentDir);
    Map<IndexKey, long[]> offsets = parseIndexMap(segmentDir, metadata);
    MappedByteBuffer buffer =
        Mmap.mapReadOnly(new File(segmentDir, SingleFileSegmentWriter.INDEX_FILE_NAME));
    return new SegmentDirectory(segmentDir, metadata, buffer, offsets);
  }

  private static Map<IndexKey, long[]> parseIndexMap(File segmentDir, SegmentMetadata metadata)
      throws IOException {
    Properties props = new Properties();
    try (InputStream in =
        Files.newInputStream(new File(segmentDir, SingleFileSegmentWriter.INDEX_MAP_FILE_NAME).toPath())) {
      props.load(in);
    }
    Map<IndexKey, long[]> offsets = new HashMap<>();
    for (String column : metadata.getColumnNames()) {
      for (IndexType indexType : IndexType.values()) {
        String prefix = column + "." + indexType.getId();
        String start = props.getProperty(prefix + ".startOffset");
        if (start == null) {
          continue;
        }
        long size = Long.parseLong(props.getProperty(prefix + ".size"));
        offsets.put(new IndexKey(column, indexType), new long[]{Long.parseLong(start), size});
      }
    }
    return offsets;
  }

  public SegmentMetadata getMetadata() {
    return _metadata;
  }

  public boolean hasIndex(String column, IndexType indexType) {
    return _offsets.containsKey(new IndexKey(column, indexType));
  }

  /**
   * Return a zero-based, read-only slice of the mapped index file for the requested entry, or
   * {@code null} if it does not exist. The slice's index 0 corresponds to the entry's first byte.
   */
  public ByteBuffer getBuffer(String column, IndexType indexType) {
    long[] loc = _offsets.get(new IndexKey(column, indexType));
    if (loc == null) {
      return null;
    }
    int start = Math.toIntExact(loc[0]);
    int size = Math.toIntExact(loc[1]);
    ByteBuffer dup = _indexBuffer.duplicate();
    dup.position(start).limit(start + size);
    return dup.slice();
  }

  public File getSegmentDir() {
    return _segmentDir;
  }

  @Override
  public void close() {
    // MappedByteBuffer is released by the GC/cleaner; nothing to close explicitly here.
  }
}
