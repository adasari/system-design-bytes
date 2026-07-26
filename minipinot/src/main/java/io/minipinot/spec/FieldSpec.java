package io.minipinot.spec;

import java.util.Objects;

/**
 * Describes a single column: its name, physical {@link DataType}, logical {@link FieldType},
 * single- vs multi-valued flag and default null value. Mirrors a simplified
 * {@code org.apache.pinot.spi.data.FieldSpec}.
 */
public final class FieldSpec {
  private final String _name;
  private final DataType _dataType;
  private final FieldType _fieldType;
  private final boolean _singleValue;
  private final Object _defaultNullValue;

  public FieldSpec(String name, DataType dataType, FieldType fieldType, boolean singleValue,
      Object defaultNullValue) {
    _name = Objects.requireNonNull(name, "name");
    _dataType = Objects.requireNonNull(dataType, "dataType");
    _fieldType = Objects.requireNonNull(fieldType, "fieldType");
    _singleValue = singleValue;
    _defaultNullValue = defaultNullValue != null ? defaultNullValue : dataType.getDefaultNullValue();
  }

  public String getName() {
    return _name;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public FieldType getFieldType() {
    return _fieldType;
  }

  public boolean isSingleValue() {
    return _singleValue;
  }

  public Object getDefaultNullValue() {
    return _defaultNullValue;
  }

  @Override
  public String toString() {
    return "FieldSpec{" + _name + ", " + _dataType + ", " + _fieldType
        + ", sv=" + _singleValue + '}';
  }
}
