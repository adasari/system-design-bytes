package io.minipinot.store;

import io.minipinot.dict.SortedDictionary;
import io.minipinot.forward.FixedBitForwardIndex;
import io.minipinot.forward.ForwardIndexEncoding;
import io.minipinot.forward.LuceneFixedBitForwardIndex;
import io.minipinot.forward.SortedForwardIndex;
import io.minipinot.invert.BitmapInvertedIndex;
import io.minipinot.invert.BloomFilter;
import io.minipinot.invert.RangeIndex;
import io.minipinot.record.GenericRow;
import io.minipinot.record.RecordReader;
import io.minipinot.spec.FieldSpec;
import io.minipinot.spec.Schema;
import io.minipinot.stats.ColumnStats;
import io.minipinot.stats.ColumnStatsCollector;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Orchestrates the offline write path end-to-end, mirroring Pinot's
 * {@code SegmentIndexCreationDriverImpl}:
 *
 * <ol>
 *   <li>Pass 1 - collect per-column statistics.</li>
 *   <li>Build a sorted dictionary per column.</li>
 *   <li>Pass 2 - resolve each value to a dictId and populate the forward index and any configured
 *       inverted / range / bloom indexes.</li>
 *   <li>Persist everything into the single-file segment layout ({@code columns.psf} + index_map +
 *       metadata.properties).</li>
 * </ol>
 *
 * <p>Single-valued columns only (multi-value is a documented future extension).
 */
public final class SegmentBuildDriver {

  public File build(Schema schema, RecordReader recordReader, String segmentName, File outputDir,
      SegmentBuildConfig config)
      throws IOException {
    List<String> columns = schema.getColumnNames();

    // ---- Pass 1: statistics ---------------------------------------------
    Map<String, ColumnStatsCollector> collectors = new LinkedHashMap<>();
    for (String column : columns) {
      collectors.put(column, new ColumnStatsCollector(schema.getFieldSpec(column)));
    }
    recordReader.rewind();
    GenericRow reuse = new GenericRow();
    while (recordReader.hasNext()) {
      recordReader.next(reuse);
      for (String column : columns) {
        collectors.get(column).collect(reuse.getValue(column));
      }
    }
    Map<String, ColumnStats> stats = new LinkedHashMap<>();
    for (String column : columns) {
      ColumnStats sealed = collectors.get(column).seal();
      if (!sealed.isSingleValue()) {
        throw new UnsupportedOperationException(
            "MiniPinot currently supports single-valued columns only: " + column);
      }
      stats.put(column, sealed);
    }

    int totalDocs = stats.get(columns.get(0)).getTotalDocs();

    // ---- Dictionaries ---------------------------------------------------
    Map<String, SortedDictionary> dictionaries = new LinkedHashMap<>();
    for (String column : columns) {
      dictionaries.put(column, SortedDictionary.build(schema.getFieldSpec(column).getDataType(),
          stats.get(column).getSortedUniqueValues()));
    }

    // ---- Pass 2: resolve dictIds per document ---------------------------
    Map<String, int[]> dictIdsPerColumn = new LinkedHashMap<>();
    for (String column : columns) {
      dictIdsPerColumn.put(column, new int[totalDocs]);
    }
    recordReader.rewind();
    int docId = 0;
    while (recordReader.hasNext()) {
      recordReader.next(reuse);
      for (String column : columns) {
        dictIdsPerColumn.get(column)[docId] = dictionaries.get(column).indexOf(reuse.getValue(column));
      }
      docId++;
    }

    // ---- Build buffers + metadata ---------------------------------------
    SingleFileSegmentWriter writer = new SingleFileSegmentWriter();
    Map<String, ColumnMetadata> columnMetadata = new LinkedHashMap<>();

    for (String column : columns) {
      FieldSpec fieldSpec = schema.getFieldSpec(column);
      ColumnStats st = stats.get(column);
      SortedDictionary dictionary = dictionaries.get(column);
      int[] dictIds = dictIdsPerColumn.get(column);
      int cardinality = st.getCardinality();

      writer.add(column, IndexType.DICTIONARY, dictionary.serialize());

      int numBits = 0;
      if (st.isSorted()) {
        SortedForwardIndex.Creator fwd = new SortedForwardIndex.Creator(cardinality);
        for (int d = 0; d < totalDocs; d++) {
          fwd.add(d, dictIds[d]);
        }
        writer.add(column, IndexType.FORWARD_INDEX, fwd.serialize());
      } else if (config.getForwardIndexEncoding() == ForwardIndexEncoding.LUCENE_DIRECT) {
        LuceneFixedBitForwardIndex.Creator fwd =
            new LuceneFixedBitForwardIndex.Creator(totalDocs, cardinality);
        for (int d = 0; d < totalDocs; d++) {
          fwd.add(dictIds[d]);
        }
        numBits = fwd.getBitsPerValue();
        writer.add(column, IndexType.FORWARD_INDEX, fwd.serialize());
      } else {
        FixedBitForwardIndex.Creator fwd = new FixedBitForwardIndex.Creator(totalDocs, cardinality);
        for (int d = 0; d < totalDocs; d++) {
          fwd.add(dictIds[d]);
        }
        numBits = fwd.getNumBits();
        writer.add(column, IndexType.FORWARD_INDEX, fwd.serialize());
      }

      // Inverted index: skipped for sorted columns (the sorted forward index already is one).
      if (config.hasInvertedIndex(column) && !st.isSorted()) {
        BitmapInvertedIndex.Creator inv = new BitmapInvertedIndex.Creator(cardinality);
        for (int d = 0; d < totalDocs; d++) {
          inv.add(d, dictIds[d]);
        }
        writer.add(column, IndexType.INVERTED_INDEX, inv.serialize());
      }

      if (config.hasRangeIndex(column)) {
        RangeIndex.Creator range = new RangeIndex.Creator(cardinality);
        for (int d = 0; d < totalDocs; d++) {
          range.add(d, dictIds[d]);
        }
        writer.add(column, IndexType.RANGE_INDEX, range.serialize());
      }

      if (config.hasBloomFilter(column)) {
        BloomFilter.Creator bloom = new BloomFilter.Creator(cardinality);
        for (int dict = 0; dict < cardinality; dict++) {
          bloom.add(dictionary.get(dict));
        }
        writer.add(column, IndexType.BLOOM_FILTER, bloom.serialize());
      }

      columnMetadata.put(column, new ColumnMetadata(column, fieldSpec.getDataType(),
          fieldSpec.isSingleValue(), true, st.isSorted(), cardinality, numBits,
          dictionary.getStride(), totalDocs, toStringOrNull(st.getMinValue()),
          toStringOrNull(st.getMaxValue())));
    }

    SegmentMetadata metadata = new SegmentMetadata(segmentName, totalDocs,
        config.getForwardIndexEncoding(), columnMetadata);
    File segmentDir = new File(outputDir, segmentName);
    writer.write(segmentDir, metadata);
    return segmentDir;
  }

  private static String toStringOrNull(Object value) {
    return value == null ? null : value.toString();
  }
}
