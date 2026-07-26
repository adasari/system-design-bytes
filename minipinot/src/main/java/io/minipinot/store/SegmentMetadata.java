package io.minipinot.store;

import io.minipinot.forward.ForwardIndexEncoding;
import io.minipinot.spec.DataType;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Segment-level metadata plus a map of {@link ColumnMetadata}, persisted as {@code
 * metadata.properties}. Mirrors Pinot's {@code SegmentMetadataImpl} and the keys in
 * {@code V1Constants.MetadataKeys}. Kept as a flat java.util.Properties for readability.
 */
public final class SegmentMetadata {
  public static final String METADATA_FILE_NAME = "metadata.properties";

  private static final String K_SEGMENT_NAME = "segment.name";
  private static final String K_TOTAL_DOCS = "segment.total.docs";
  private static final String K_COLUMNS = "segment.column.names";
  private static final String K_FORWARD_INDEX_ENCODING = "segment.forwardIndexEncoding";

  private final String _segmentName;
  private final int _totalDocs;
  private final ForwardIndexEncoding _forwardIndexEncoding;
  private final Map<String, ColumnMetadata> _columns;

  public SegmentMetadata(String segmentName, int totalDocs, Map<String, ColumnMetadata> columns) {
    this(segmentName, totalDocs, ForwardIndexEncoding.HANDCRAFTED, columns);
  }

  public SegmentMetadata(String segmentName, int totalDocs, ForwardIndexEncoding forwardIndexEncoding,
      Map<String, ColumnMetadata> columns) {
    _segmentName = segmentName;
    _totalDocs = totalDocs;
    _forwardIndexEncoding = forwardIndexEncoding;
    _columns = columns;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public int getTotalDocs() {
    return _totalDocs;
  }

  public ForwardIndexEncoding getForwardIndexEncoding() {
    return _forwardIndexEncoding;
  }

  public ColumnMetadata getColumnMetadata(String column) {
    return _columns.get(column);
  }

  public List<String> getColumnNames() {
    return new ArrayList<>(_columns.keySet());
  }

  public void write(File segmentDir)
      throws IOException {
    Properties props = new Properties();
    props.setProperty(K_SEGMENT_NAME, _segmentName);
    props.setProperty(K_TOTAL_DOCS, Integer.toString(_totalDocs));
    props.setProperty(K_FORWARD_INDEX_ENCODING, _forwardIndexEncoding.name());
    props.setProperty(K_COLUMNS, String.join(",", _columns.keySet()));
    for (ColumnMetadata cm : _columns.values()) {
      String p = "column." + cm.getName() + ".";
      props.setProperty(p + "dataType", cm.getDataType().name());
      props.setProperty(p + "singleValue", Boolean.toString(cm.isSingleValue()));
      props.setProperty(p + "hasDictionary", Boolean.toString(cm.hasDictionary()));
      props.setProperty(p + "sorted", Boolean.toString(cm.isSorted()));
      props.setProperty(p + "cardinality", Integer.toString(cm.getCardinality()));
      props.setProperty(p + "numBitsPerElement", Integer.toString(cm.getNumBitsPerElement()));
      props.setProperty(p + "dictionaryStride", Integer.toString(cm.getDictionaryStride()));
      props.setProperty(p + "totalDocs", Integer.toString(cm.getTotalDocs()));
      props.setProperty(p + "minValue", cm.getMinValue() == null ? "" : cm.getMinValue());
      props.setProperty(p + "maxValue", cm.getMaxValue() == null ? "" : cm.getMaxValue());
    }
    try (OutputStream out = Files.newOutputStream(new File(segmentDir, METADATA_FILE_NAME).toPath())) {
      props.store(out, "MiniPinot segment metadata");
    }
  }

  public static SegmentMetadata read(File segmentDir)
      throws IOException {
    Properties props = new Properties();
    try (InputStream in = Files.newInputStream(new File(segmentDir, METADATA_FILE_NAME).toPath())) {
      props.load(in);
    }
    String segmentName = props.getProperty(K_SEGMENT_NAME);
    int totalDocs = Integer.parseInt(props.getProperty(K_TOTAL_DOCS));
    ForwardIndexEncoding forwardIndexEncoding = ForwardIndexEncoding.valueOf(
        props.getProperty(K_FORWARD_INDEX_ENCODING, ForwardIndexEncoding.HANDCRAFTED.name()));
    Map<String, ColumnMetadata> columns = new LinkedHashMap<>();
    String columnList = props.getProperty(K_COLUMNS, "");
    if (!columnList.isEmpty()) {
      for (String name : columnList.split(",")) {
        String p = "column." + name + ".";
        ColumnMetadata cm = new ColumnMetadata(name,
            DataType.valueOf(props.getProperty(p + "dataType")),
            Boolean.parseBoolean(props.getProperty(p + "singleValue")),
            Boolean.parseBoolean(props.getProperty(p + "hasDictionary")),
            Boolean.parseBoolean(props.getProperty(p + "sorted")),
            Integer.parseInt(props.getProperty(p + "cardinality")),
            Integer.parseInt(props.getProperty(p + "numBitsPerElement")),
            Integer.parseInt(props.getProperty(p + "dictionaryStride")),
            Integer.parseInt(props.getProperty(p + "totalDocs")),
            emptyToNull(props.getProperty(p + "minValue")),
            emptyToNull(props.getProperty(p + "maxValue")));
        columns.put(name, cm);
      }
    }
    return new SegmentMetadata(segmentName, totalDocs, forwardIndexEncoding, columns);
  }

  private static String emptyToNull(String s) {
    return s == null || s.isEmpty() ? null : s;
  }
}
