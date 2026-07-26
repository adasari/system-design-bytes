package io.minipinot.spec;

/**
 * The logical role a column plays in a table. Mirrors
 * {@code org.apache.pinot.spi.data.FieldSpec.FieldType} (subset).
 *
 * <ul>
 *   <li>DIMENSION - grouping / filtering columns (usually dictionary encoded, may have inverted index)</li>
 *   <li>METRIC - numeric measures aggregated at query time (SUM/MIN/MAX...)</li>
 *   <li>DATE_TIME - the primary time column, also used for segment start/end time</li>
 * </ul>
 */
public enum FieldType {
  DIMENSION,
  METRIC,
  DATE_TIME
}
