package io.minipinot.store;

import io.minipinot.spec.DataType;

/**
 * Per-column metadata persisted in {@code metadata.properties}. These values are everything the
 * read path needs to reconstruct a column's readers without touching the data buffers: the type,
 * whether it is dictionary encoded and/or sorted, the forward-index bit width, dictionary stride,
 * cardinality and min/max. Mirrors Pinot's {@code ColumnMetadataImpl}.
 */
public final class ColumnMetadata {
  private final String _name;
  private final DataType _dataType;
  private final boolean _singleValue;
  private final boolean _hasDictionary;
  private final boolean _sorted;
  private final int _cardinality;
  private final int _numBitsPerElement;
  private final int _dictionaryStride;
  private final int _totalDocs;
  private final String _minValue;
  private final String _maxValue;

  public ColumnMetadata(String name, DataType dataType, boolean singleValue, boolean hasDictionary,
      boolean sorted, int cardinality, int numBitsPerElement, int dictionaryStride, int totalDocs,
      String minValue, String maxValue) {
    _name = name;
    _dataType = dataType;
    _singleValue = singleValue;
    _hasDictionary = hasDictionary;
    _sorted = sorted;
    _cardinality = cardinality;
    _numBitsPerElement = numBitsPerElement;
    _dictionaryStride = dictionaryStride;
    _totalDocs = totalDocs;
    _minValue = minValue;
    _maxValue = maxValue;
  }

  public String getName() {
    return _name;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public boolean isSingleValue() {
    return _singleValue;
  }

  public boolean hasDictionary() {
    return _hasDictionary;
  }

  public boolean isSorted() {
    return _sorted;
  }

  public int getCardinality() {
    return _cardinality;
  }

  public int getNumBitsPerElement() {
    return _numBitsPerElement;
  }

  public int getDictionaryStride() {
    return _dictionaryStride;
  }

  public int getTotalDocs() {
    return _totalDocs;
  }

  public String getMinValue() {
    return _minValue;
  }

  public String getMaxValue() {
    return _maxValue;
  }
}
