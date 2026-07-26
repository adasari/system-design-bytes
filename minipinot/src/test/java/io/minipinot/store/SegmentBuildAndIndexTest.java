package io.minipinot.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.FixedBitForwardIndex;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.forward.SortedForwardIndex;
import io.minipinot.invert.BitmapInvertedIndex;
import io.minipinot.invert.BloomFilter;
import io.minipinot.invert.RangeIndex;
import io.minipinot.record.CsvRecordReader;
import io.minipinot.record.GenericRow;
import io.minipinot.spec.DataType;
import io.minipinot.spec.Schema;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/** Phase 2 (on-disk single-file layout) and Phase 3 (indices) integration tests. */
public class SegmentBuildAndIndexTest {

  private static final File SCHEMA_FILE =
      new File("src/main/resources/samples/events_schema.json");
  private static final File CSV_FILE =
      new File("src/main/resources/samples/events.csv");

  private Schema _schema;
  private List<GenericRow> _rows;

  @BeforeEach
  public void setUp()
      throws Exception {
    _schema = Schema.fromJsonFile(SCHEMA_FILE);
    _rows = CsvRecordReader.readAll(CSV_FILE, _schema);
  }

  private File buildSegment(File tempDir)
      throws Exception {
    SegmentBuildConfig config = new SegmentBuildConfig()
        .withInvertedIndex("browser", "device")
        .withRangeIndex("clicks", "ts")
        .withBloomFilter("country");
    try (CsvRecordReader reader = new CsvRecordReader(CSV_FILE, _schema)) {
      return new SegmentBuildDriver().build(_schema, reader, "events_seg", tempDir, config);
    }
  }

  private SortedDictionary dictionary(SegmentDirectory dir, String column) {
    ColumnMetadata cm = dir.getMetadata().getColumnMetadata(column);
    return new SortedDictionary(cm.getDataType(), cm.getCardinality(), cm.getDictionaryStride(),
        dir.getBuffer(column, IndexType.DICTIONARY), 0);
  }

  private ForwardIndexReader forwardIndex(SegmentDirectory dir, String column) {
    ColumnMetadata cm = dir.getMetadata().getColumnMetadata(column);
    ByteBuffer buffer = dir.getBuffer(column, IndexType.FORWARD_INDEX);
    if (cm.isSorted()) {
      return new SortedForwardIndex.Reader(buffer, 0, cm.getCardinality(), cm.getTotalDocs());
    }
    if (dir.getMetadata().getForwardIndexEncoding()
        == io.minipinot.forward.ForwardIndexEncoding.LUCENE_DIRECT) {
      return new io.minipinot.forward.LuceneFixedBitForwardIndex.Reader(
          buffer, cm.getTotalDocs(), cm.getNumBitsPerElement());
    }
    return new FixedBitForwardIndex.Reader(buffer, 0, cm.getTotalDocs(), cm.getNumBitsPerElement());
  }

  @Test
  public void writesSingleFileLayoutAndMetadata()
      throws Exception {
    File tempDir = java.nio.file.Files.createTempDirectory("mp").toFile();
    File segmentDir = buildSegment(tempDir);

    assertTrue(new File(segmentDir, SingleFileSegmentWriter.INDEX_FILE_NAME).exists());
    assertTrue(new File(segmentDir, SingleFileSegmentWriter.INDEX_MAP_FILE_NAME).exists());
    assertTrue(new File(segmentDir, SegmentMetadata.METADATA_FILE_NAME).exists());

    try (SegmentDirectory dir = SegmentDirectory.open(segmentDir)) {
      SegmentMetadata metadata = dir.getMetadata();
      assertEquals("events_seg", metadata.getSegmentName());
      assertEquals(10, metadata.getTotalDocs());
      assertEquals(6, metadata.getColumnNames().size());
      // ts is strictly ascending -> stored as a sorted forward index.
      assertTrue(metadata.getColumnMetadata("ts").isSorted());
      assertFalse(metadata.getColumnMetadata("country").isSorted());
      assertEquals(DataType.DOUBLE, metadata.getColumnMetadata("revenue").getDataType());
    }
  }

  @Test
  public void forwardIndexAndDictionaryRoundTripFromMmap()
      throws Exception {
    File tempDir = java.nio.file.Files.createTempDirectory("mp").toFile();
    File segmentDir = buildSegment(tempDir);

    try (SegmentDirectory dir = SegmentDirectory.open(segmentDir)) {
      for (String column : _schema.getColumnNames()) {
        SortedDictionary dictionary = dictionary(dir, column);
        ForwardIndexReader fwd = forwardIndex(dir, column);
        for (int docId = 0; docId < _rows.size(); docId++) {
          Object expected = _rows.get(docId).getValue(column);
          Object actual = dictionary.get(fwd.getDictId(docId));
          assertEquals(String.valueOf(expected), String.valueOf(actual),
              "round-trip mismatch column=" + column + " docId=" + docId);
        }
      }
    }
  }

  @Test
  public void invertedIndexMatchesBruteForce()
      throws Exception {
    File tempDir = java.nio.file.Files.createTempDirectory("mp").toFile();
    File segmentDir = buildSegment(tempDir);

    try (SegmentDirectory dir = SegmentDirectory.open(segmentDir)) {
      SortedDictionary dictionary = dictionary(dir, "browser");
      BitmapInvertedIndex.Reader inverted =
          new BitmapInvertedIndex.Reader(dir.getBuffer("browser", IndexType.INVERTED_INDEX));

      int chromeDictId = dictionary.indexOf("Chrome");
      ImmutableRoaringBitmap docIds = inverted.getDocIds(chromeDictId);

      List<Integer> expected = new ArrayList<>();
      for (int docId = 0; docId < _rows.size(); docId++) {
        if ("Chrome".equals(_rows.get(docId).getValue("browser"))) {
          expected.add(docId);
        }
      }
      List<Integer> actual = new ArrayList<>();
      docIds.forEach((org.roaringbitmap.IntConsumer) actual::add);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void rangeIndexPlusForwardRecheckIsExact()
      throws Exception {
    File tempDir = java.nio.file.Files.createTempDirectory("mp").toFile();
    File segmentDir = buildSegment(tempDir);

    try (SegmentDirectory dir = SegmentDirectory.open(segmentDir)) {
      SortedDictionary dictionary = dictionary(dir, "clicks");
      ForwardIndexReader fwd = forwardIndex(dir, "clicks");
      RangeIndex.Reader range =
          new RangeIndex.Reader(dir.getBuffer("clicks", IndexType.RANGE_INDEX));

      long low = 2L;
      long high = 4L;
      int lowDictId = insertionAtLeast(dictionary, low);
      int highDictId = insertionAtMost(dictionary, high);

      RangeIndex.RangeMatch match = range.query(lowDictId, highDictId);
      MutableRoaringBitmap result = new MutableRoaringBitmap();
      result.or(match._fullyMatching);
      // Re-check boundary docs against the forward index (exactly what the query engine does).
      match._partiallyMatching.forEach((org.roaringbitmap.IntConsumer) docId -> {
        long value = (Long) dictionary.get(fwd.getDictId(docId));
        if (value >= low && value <= high) {
          result.add(docId);
        }
      });

      TreeSet<Integer> expected = new TreeSet<>();
      for (int docId = 0; docId < _rows.size(); docId++) {
        long value = (Long) _rows.get(docId).getValue("clicks");
        if (value >= low && value <= high) {
          expected.add(docId);
        }
      }
      TreeSet<Integer> actual = new TreeSet<>();
      result.forEach((org.roaringbitmap.IntConsumer) actual::add);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void bloomFilterHasNoFalseNegatives()
      throws Exception {
    File tempDir = java.nio.file.Files.createTempDirectory("mp").toFile();
    File segmentDir = buildSegment(tempDir);

    try (SegmentDirectory dir = SegmentDirectory.open(segmentDir)) {
      BloomFilter.Reader bloom =
          new BloomFilter.Reader(dir.getBuffer("country", IndexType.BLOOM_FILTER));

      // Guaranteed property: every value actually present must test positive.
      for (String value : new TreeSet<>(List.of("US", "IN", "DE"))) {
        assertTrue(bloom.mightContain(value), "false negative for present value " + value);
      }

      // Absent values should mostly be rejected (false positives allowed, but rare).
      int falsePositives = 0;
      int trials = 200;
      for (int i = 0; i < trials; i++) {
        if (bloom.mightContain("absent-" + i)) {
          falsePositives++;
        }
      }
      assertTrue(falsePositives < trials / 2,
          "too many false positives: " + falsePositives + "/" + trials);
    }
  }

  /** First dictId whose value is >= bound (insertion point convention). */
  private static int insertionAtLeast(SortedDictionary dictionary, Object bound) {
    int idx = dictionary.indexOf(bound);
    return idx >= 0 ? idx : -(idx) - 1;
  }

  /** Last dictId whose value is <= bound. */
  private static int insertionAtMost(SortedDictionary dictionary, Object bound) {
    int idx = dictionary.indexOf(bound);
    return idx >= 0 ? idx : -(idx) - 2;
  }
}
