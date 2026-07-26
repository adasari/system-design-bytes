package io.minipinot.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.FixedBitForwardIndex;
import io.minipinot.forward.ForwardIndexEncoding;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.forward.LuceneFixedBitForwardIndex;
import io.minipinot.record.CsvRecordReader;
import io.minipinot.record.GenericRow;
import io.minipinot.spec.Schema;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Tests for the production-grade Lucene {@code DirectWriter}/{@code DirectReader} forward index. */
public class LuceneForwardIndexTest {

  private static final File SCHEMA_FILE =
      new File("src/main/resources/samples/events_schema.json");
  private static final File CSV_FILE =
      new File("src/main/resources/samples/events.csv");

  @Test
  public void luceneRoundTripsRandomDictIds() {
    int cardinality = 1000;
    int numDocs = 5000;
    Random random = new Random(42);
    int[] dictIds = new int[numDocs];
    for (int d = 0; d < numDocs; d++) {
      dictIds[d] = random.nextInt(cardinality);
    }

    LuceneFixedBitForwardIndex.Creator creator =
        new LuceneFixedBitForwardIndex.Creator(numDocs, cardinality);
    for (int dictId : dictIds) {
      creator.add(dictId);
    }
    byte[] bytes = creator.serialize();

    LuceneFixedBitForwardIndex.Reader reader =
        new LuceneFixedBitForwardIndex.Reader(ByteBuffer.wrap(bytes), numDocs,
            creator.getBitsPerValue());
    for (int d = 0; d < numDocs; d++) {
      assertEquals(dictIds[d], reader.getDictId(d), "mismatch at doc " + d);
    }
  }

  @Test
  public void luceneAndHandcraftedReturnIdenticalDictIds() {
    int cardinality = 300;
    int numDocs = 2048;
    Random random = new Random(7);
    int[] dictIds = new int[numDocs];
    for (int d = 0; d < numDocs; d++) {
      dictIds[d] = random.nextInt(cardinality);
    }

    FixedBitForwardIndex.Creator handcrafted =
        new FixedBitForwardIndex.Creator(numDocs, cardinality);
    LuceneFixedBitForwardIndex.Creator lucene =
        new LuceneFixedBitForwardIndex.Creator(numDocs, cardinality);
    for (int dictId : dictIds) {
      handcrafted.add(dictId);
      lucene.add(dictId);
    }
    ForwardIndexReader hcReader = new FixedBitForwardIndex.Reader(
        ByteBuffer.wrap(handcrafted.serialize()), 0, numDocs, handcrafted.getNumBits());
    ForwardIndexReader luceneReader = new LuceneFixedBitForwardIndex.Reader(
        ByteBuffer.wrap(lucene.serialize()), numDocs, lucene.getBitsPerValue());

    for (int d = 0; d < numDocs; d++) {
      assertEquals(hcReader.getDictId(d), luceneReader.getDictId(d), "mismatch at doc " + d);
    }
    // Lucene rounds bit width up to a supported size, so it never uses fewer bits than the minimum.
    assertTrue(lucene.getBitsPerValue() >= handcrafted.getNumBits(),
        "lucene bits " + lucene.getBitsPerValue() + " < handcrafted " + handcrafted.getNumBits());
  }

  @Test
  public void segmentBuiltWithLuceneEncodingRoundTrips()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    List<GenericRow> rows = CsvRecordReader.readAll(CSV_FILE, schema);

    File tempDir = java.nio.file.Files.createTempDirectory("mp-lucene").toFile();
    SegmentBuildConfig config = new SegmentBuildConfig()
        .withForwardIndexEncoding(ForwardIndexEncoding.LUCENE_DIRECT)
        .withInvertedIndex("browser", "device")
        .withRangeIndex("clicks", "ts")
        .withBloomFilter("country");
    File segmentDir;
    try (CsvRecordReader reader = new CsvRecordReader(CSV_FILE, schema)) {
      segmentDir = new SegmentBuildDriver().build(schema, reader, "events_lucene", tempDir, config);
    }

    try (SegmentDirectory dir = SegmentDirectory.open(segmentDir)) {
      assertEquals(ForwardIndexEncoding.LUCENE_DIRECT,
          dir.getMetadata().getForwardIndexEncoding());
      for (String column : schema.getColumnNames()) {
        ColumnMetadata cm = dir.getMetadata().getColumnMetadata(column);
        SortedDictionary dictionary = new SortedDictionary(cm.getDataType(), cm.getCardinality(),
            cm.getDictionaryStride(), dir.getBuffer(column, IndexType.DICTIONARY), 0);
        ForwardIndexReader fwd;
        ByteBuffer buffer = dir.getBuffer(column, IndexType.FORWARD_INDEX);
        if (cm.isSorted()) {
          fwd = new io.minipinot.forward.SortedForwardIndex.Reader(
              buffer, 0, cm.getCardinality(), cm.getTotalDocs());
        } else {
          fwd = new LuceneFixedBitForwardIndex.Reader(
              buffer, cm.getTotalDocs(), cm.getNumBitsPerElement());
        }
        for (int docId = 0; docId < rows.size(); docId++) {
          Object expected = rows.get(docId).getValue(column);
          Object actual = dictionary.get(fwd.getDictId(docId));
          assertEquals(String.valueOf(expected), String.valueOf(actual),
              "round-trip mismatch column=" + column + " docId=" + docId);
        }
      }
    }
  }
}
