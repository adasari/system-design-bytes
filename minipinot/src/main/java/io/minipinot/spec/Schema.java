package io.minipinot.spec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The table schema: an ordered collection of {@link FieldSpec}s plus a name. Mirrors a
 * simplified {@code org.apache.pinot.spi.data.Schema} and is loaded from a Pinot-style JSON:
 *
 * <pre>
 * {
 *   "schemaName": "events",
 *   "dimensionFieldSpecs": [ {"name": "country", "dataType": "STRING"} ],
 *   "metricFieldSpecs":    [ {"name": "clicks",  "dataType": "LONG"} ],
 *   "dateTimeFieldSpecs":  [ {"name": "ts",      "dataType": "LONG"} ]
 * }
 * </pre>
 */
public final class Schema {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String _name;
  private final Map<String, FieldSpec> _fieldSpecs = new LinkedHashMap<>();

  public Schema(String name, List<FieldSpec> fieldSpecs) {
    _name = name;
    for (FieldSpec fieldSpec : fieldSpecs) {
      _fieldSpecs.put(fieldSpec.getName(), fieldSpec);
    }
  }

  public String getName() {
    return _name;
  }

  public FieldSpec getFieldSpec(String column) {
    return _fieldSpecs.get(column);
  }

  /** Column names in schema-declaration order. */
  public List<String> getColumnNames() {
    return new ArrayList<>(_fieldSpecs.keySet());
  }

  public List<FieldSpec> getFieldSpecs() {
    return new ArrayList<>(_fieldSpecs.values());
  }

  public int size() {
    return _fieldSpecs.size();
  }

  public static Schema fromJsonFile(File file)
      throws IOException {
    return fromJson(MAPPER.readTree(file));
  }

  public static Schema fromJsonString(String json)
      throws IOException {
    return fromJson(MAPPER.readTree(json));
  }

  private static Schema fromJson(JsonNode root) {
    String name = root.path("schemaName").asText("unnamed");
    List<FieldSpec> specs = new ArrayList<>();
    readSpecs(root, "dimensionFieldSpecs", FieldType.DIMENSION, specs);
    readSpecs(root, "metricFieldSpecs", FieldType.METRIC, specs);
    readSpecs(root, "dateTimeFieldSpecs", FieldType.DATE_TIME, specs);
    return new Schema(name, specs);
  }

  private static void readSpecs(JsonNode root, String field, FieldType fieldType,
      List<FieldSpec> out) {
    JsonNode array = root.path(field);
    if (array.isMissingNode() || array.isNull()) {
      return;
    }
    for (JsonNode node : array) {
      String name = node.path("name").asText();
      DataType dataType = DataType.valueOf(node.path("dataType").asText().toUpperCase());
      boolean singleValue = node.path("singleValueField").asBoolean(true);
      Object defaultNull = null;
      if (node.hasNonNull("defaultNullValue")) {
        defaultNull = dataType.parse(node.get("defaultNullValue").asText());
      }
      out.add(new FieldSpec(name, dataType, fieldType, singleValue, defaultNull));
    }
  }

  @Override
  public String toString() {
    return "Schema{" + _name + ", columns=" + Collections.unmodifiableSet(_fieldSpecs.keySet()) + '}';
  }
}
