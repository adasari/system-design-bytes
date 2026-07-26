package io.minipinot.write;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.FixedBitForwardIndex;
import io.minipinot.forward.ForwardIndexReader;
import io.minipinot.forward.SortedForwardIndex;
import io.minipinot.record.CsvRecordReader;
import io.minipinot.record.GenericRow;
import io.minipinot.spec.FieldSpec;
import io.minipinot.spec.Schema;
import io.minipinot.stats.ColumnStats;
import io.minipinot.stats.ColumnStatsCollector;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Phase 1: the write path. Runs the two-pass build (stats -> dictionary -> forward index) for a
 * dictionary-encoded unsorted column and a sorted column, then round-trips every document value.
 */
public class WritePathTest {

  private static final File SCHEMA_FILE =
      new File("src/main/resources/samples/events_schema.json");
  private static final File CSV_FILE =
      new File("src/main/resources/samples/events.csv");

  private static ColumnStats collectStats(List<GenericRow> rows, FieldSpec spec) {
    ColumnStatsCollector collector = new ColumnStatsCollector(spec);
    for (GenericRow row : rows) {
      collector.collect(row.getValue(spec.getName()));
    }
    return collector.seal();
  }

  @Test
  public void bitPackedForwardIndexRoundTrips()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    List<GenericRow> rows = CsvRecordReader.readAll(CSV_FILE, schema);
    FieldSpec spec = schema.getFieldSpec("country");

    // Pass 1: stats. 3 distinct countries (US, IN, DE), column is not sorted.
    ColumnStats stats = collectStats(rows, spec);
    assertEquals(3, stats.getCardinality());
    assertFalse(stats.isSorted());

    // Dictionary from sorted distinct values.
    SortedDictionary dictionary =
        SortedDictionary.build(spec.getDataType(), stats.getSortedUniqueValues());
    assertEquals("DE", dictionary.get(0));
    assertEquals("IN", dictionary.get(1));
    assertEquals("US", dictionary.get(2));

    // Pass 2: forward index. Cardinality 3 -> 2 bits per document.
    FixedBitForwardIndex.Creator creator =
        new FixedBitForwardIndex.Creator(rows.size(), stats.getCardinality());
    assertEquals(2, creator.getNumBits());
    for (GenericRow row : rows) {
      creator.add(dictionary.indexOf(row.getValue("country")));
    }
    ByteBuffer buffer = ByteBuffer.wrap(creator.serialize());
    ForwardIndexReader reader =
        new FixedBitForwardIndex.Reader(buffer, 0, rows.size(), creator.getNumBits());

    // Round-trip: value == dictionary.get(forwardIndex.getDictId(docId)).
    for (int docId = 0; docId < rows.size(); docId++) {
      Object expected = rows.get(docId).getValue("country");
      Object actual = dictionary.get(reader.getDictId(docId));
      assertEquals(expected, actual, "mismatch at docId " + docId);
    }
  }

  @Test
  public void sortedForwardIndexRoundTripsAndYieldsRanges()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    List<GenericRow> rows = CsvRecordReader.readAll(CSV_FILE, schema);
    FieldSpec spec = schema.getFieldSpec("ts");

    ColumnStats stats = collectStats(rows, spec);
    assertTrue(stats.isSorted(), "ts is strictly ascending in the sample");
    assertEquals(rows.size(), stats.getCardinality());

    SortedDictionary dictionary =
        SortedDictionary.build(spec.getDataType(), stats.getSortedUniqueValues());
    SortedForwardIndex.Creator creator =
        new SortedForwardIndex.Creator(stats.getCardinality());
    for (int docId = 0; docId < rows.size(); docId++) {
      creator.add(docId, dictionary.indexOf(rows.get(docId).getValue("ts")));
    }
    ByteBuffer buffer = ByteBuffer.wrap(creator.serialize());
    SortedForwardIndex.Reader reader =
        new SortedForwardIndex.Reader(buffer, 0, stats.getCardinality(), rows.size());

    for (int docId = 0; docId < rows.size(); docId++) {
      Object expected = rows.get(docId).getValue("ts");
      Object actual = dictionary.get(reader.getDictId(docId));
      assertEquals(expected, actual, "mismatch at docId " + docId);
    }

    // Each distinct ts appears in exactly one document -> range [docId, docId].
    int dictIdOfFirst = dictionary.indexOf(rows.get(0).getValue("ts"));
    assertArrayEquals(new int[]{0, 0}, reader.getDocIdRange(dictIdOfFirst));
  }

  @Test
  public void dictionaryBinarySearchMatchesArraysConvention()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    List<GenericRow> rows = CsvRecordReader.readAll(CSV_FILE, schema);
    FieldSpec spec = schema.getFieldSpec("country");
    ColumnStats stats = collectStats(rows, spec);
    SortedDictionary dictionary =
        SortedDictionary.build(spec.getDataType(), stats.getSortedUniqueValues());

    assertEquals(2, dictionary.indexOf("US"));
    // Absent value returns -(insertionPoint)-1. "AA" sorts before everything -> -1.
    assertTrue(dictionary.indexOf("AA") < 0);
    assertEquals(-1, dictionary.indexOf("AA"));
  }
}
