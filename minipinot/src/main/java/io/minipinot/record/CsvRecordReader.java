package io.minipinot.record;

import io.minipinot.spec.DataType;
import io.minipinot.spec.FieldSpec;
import io.minipinot.spec.Schema;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal CSV {@link RecordReader}. The first line is a header of column names; each
 * subsequent line is a record. Multi-valued columns split their cell on {@code mvDelimiter}.
 *
 * <p>Deliberately simple (no quoted-comma handling) - the focus of MiniPinot is the segment
 * engine, not CSV parsing. Rows are held in memory to support {@link #rewind()} for the
 * two-pass (stats, then index) build.
 */
public final class CsvRecordReader implements RecordReader {
  private final Schema _schema;
  private final char _mvDelimiter;
  private final List<String[]> _rows = new ArrayList<>();
  private String[] _header;
  private int _cursor;

  public CsvRecordReader(File csvFile, Schema schema)
      throws IOException {
    this(csvFile, schema, ',', ';');
  }

  public CsvRecordReader(File csvFile, Schema schema, char delimiter, char mvDelimiter)
      throws IOException {
    _schema = schema;
    _mvDelimiter = mvDelimiter;
    try (BufferedReader reader = Files.newBufferedReader(csvFile.toPath(), StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      if (line == null) {
        throw new IOException("Empty CSV file: " + csvFile);
      }
      _header = split(line, delimiter);
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        _rows.add(split(line, delimiter));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return _cursor < _rows.size();
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    reuse.clear();
    String[] tokens = _rows.get(_cursor++);
    for (int i = 0; i < _header.length; i++) {
      String column = _header[i];
      FieldSpec fieldSpec = _schema.getFieldSpec(column);
      if (fieldSpec == null) {
        continue;
      }
      String rawToken = i < tokens.length ? tokens[i] : null;
      reuse.putValue(column, decode(fieldSpec, rawToken));
    }
    return reuse;
  }

  private Object decode(FieldSpec fieldSpec, String rawToken) {
    DataType dataType = fieldSpec.getDataType();
    if (rawToken == null || rawToken.isEmpty()) {
      return fieldSpec.getDefaultNullValue();
    }
    if (fieldSpec.isSingleValue()) {
      return dataType.parse(rawToken);
    }
    String[] parts = split(rawToken, _mvDelimiter);
    Object[] values = new Object[parts.length];
    for (int j = 0; j < parts.length; j++) {
      values[j] = dataType.parse(parts[j]);
    }
    return values;
  }

  @Override
  public void rewind() {
    _cursor = 0;
  }

  @Override
  public void close() {
    _rows.clear();
  }

  private static String[] split(String line, char delimiter) {
    List<String> out = new ArrayList<>();
    int start = 0;
    for (int i = 0; i < line.length(); i++) {
      if (line.charAt(i) == delimiter) {
        out.add(line.substring(start, i));
        start = i + 1;
      }
    }
    out.add(line.substring(start));
    return out.toArray(new String[0]);
  }

  public String[] getHeader() {
    return _header.clone();
  }

  /** Convenience for tests: read every row into memory. */
  public static List<GenericRow> readAll(File csvFile, Schema schema) {
    try (CsvRecordReader reader = new CsvRecordReader(csvFile, schema)) {
      List<GenericRow> rows = new ArrayList<>();
      while (reader.hasNext()) {
        GenericRow row = new GenericRow();
        reader.next(row);
        rows.add(row);
      }
      return rows;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
