package io.minipinot.spec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minipinot.record.CsvRecordReader;
import io.minipinot.record.GenericRow;
import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Phase 0: schema loading and CSV record reading. */
public class SchemaAndRecordTest {

  private static final File SCHEMA_FILE =
      new File("src/main/resources/samples/events_schema.json");
  private static final File CSV_FILE =
      new File("src/main/resources/samples/events.csv");

  @Test
  public void loadsSchemaFromJson()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    assertEquals("events", schema.getName());
    assertEquals(6, schema.size());
    assertEquals(DataType.STRING, schema.getFieldSpec("country").getDataType());
    assertEquals(FieldType.METRIC, schema.getFieldSpec("clicks").getFieldType());
    assertEquals(DataType.DOUBLE, schema.getFieldSpec("revenue").getDataType());
    assertEquals(FieldType.DATE_TIME, schema.getFieldSpec("ts").getFieldType());
    assertTrue(schema.getFieldSpec("country").isSingleValue());
  }

  @Test
  public void readsCsvIntoTypedRows()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    List<GenericRow> rows = CsvRecordReader.readAll(CSV_FILE, schema);
    assertEquals(10, rows.size());

    GenericRow first = rows.get(0);
    assertEquals("US", first.getValue("country"));
    assertEquals(3L, first.getValue("clicks"));
    assertEquals(1.50, (double) first.getValue("revenue"), 1e-9);
    assertEquals(1000L, first.getValue("ts"));
    assertFalse(first.isNullValue("country"));
  }

  @Test
  public void rewindEnablesTwoPasses()
      throws Exception {
    Schema schema = Schema.fromJsonFile(SCHEMA_FILE);
    try (CsvRecordReader reader = new CsvRecordReader(CSV_FILE, schema)) {
      GenericRow reuse = new GenericRow();
      int firstPass = 0;
      while (reader.hasNext()) {
        reader.next(reuse);
        firstPass++;
      }
      reader.rewind();
      int secondPass = 0;
      while (reader.hasNext()) {
        reader.next(reuse);
        secondPass++;
      }
      assertEquals(firstPass, secondPass);
      assertEquals(10, secondPass);
    }
  }
}
