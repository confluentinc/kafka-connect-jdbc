package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NUMERIC_PRECISION_MAPPING_CONFIG;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataConverterTest {
    private static ResultSetMetaData mockNumericSchema() throws SQLException {
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        int index = 1; // JDBC column ids start from 1

        when(metadata.getColumnType(anyInt())).thenReturn(Types.NUMERIC);
        when(metadata.getScale(anyInt())).thenReturn(0); // integer

        when(metadata.getPrecision(eq(index++))).thenReturn(3);

        when(metadata.getPrecision(eq(index))).thenReturn(3);
        when(metadata.isNullable(eq(index++))).thenReturn(ResultSetMetaData.columnNullable);

        when(metadata.getPrecision(eq(index++))).thenReturn(5);

        when(metadata.getPrecision(eq(index))).thenReturn(5);
        when(metadata.isNullable(eq(index++))).thenReturn(ResultSetMetaData.columnNullable);

        when(metadata.getPrecision(eq(index++))).thenReturn(10);

        when(metadata.getPrecision(eq(index))).thenReturn(10);
        when(metadata.isNullable(eq(index++))).thenReturn(ResultSetMetaData.columnNullable);

        when(metadata.getPrecision(eq(index++))).thenReturn(19);

        when(metadata.getPrecision(eq(index))).thenReturn(19);
        when(metadata.isNullable(eq(index++))).thenReturn(ResultSetMetaData.columnNullable);

        when(metadata.getPrecision(eq(index++))).thenReturn(20);

        when(metadata.getPrecision(eq(index))).thenReturn(20);
        when(metadata.isNullable(eq(index++))).thenReturn(ResultSetMetaData.columnNullable);

        when(metadata.getColumnCount()).thenReturn(index);
        return metadata;
    }

    private static Map<String, String> getProperties() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "someTopicPrefix");
        properties.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "someConnectionUrl");
        properties.put(JdbcSourceTaskConfig.TABLES_CONFIG, "someTables");
        return properties;
    }

    @Test
    public void numericIsMappedByPrecisionViaFlag() throws SQLException {
        ResultSetMetaData metadata = mockNumericSchema();
        Map<String, String> properties = getProperties();
        properties.put(NUMERIC_PRECISION_MAPPING_CONFIG, Boolean.TRUE.toString());
        JdbcSourceTaskConfig config = new JdbcSourceTaskConfig(properties);
        Schema schema = new DataConverter(config).convertSchema("mock", metadata);
        List<Field> fields = schema.fields();
        assertEquals(metadata.getColumnCount(), fields.size());
        int index = 0;
        assertEquals(Schema.Type.INT8, fields.get(index).schema().type());
        assertFalse(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT8, fields.get(index).schema().type());
        assertTrue(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT16, fields.get(index).schema().type());
        assertFalse(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT16, fields.get(index).schema().type());
        assertTrue(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT32, fields.get(index).schema().type());
        assertFalse(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT32, fields.get(index).schema().type());
        assertTrue(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT64, fields.get(index).schema().type());
        assertFalse(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.INT64, fields.get(index).schema().type());
        assertTrue(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.BYTES, fields.get(index).schema().type());
        assertFalse(fields.get(index++).schema().isOptional());

        assertEquals(Schema.Type.BYTES, fields.get(index).schema().type());
        assertTrue(fields.get(index).schema().isOptional());
    }

    @Test
    public void numericIsMappedToDecimalByDefault() throws SQLException {
        Map<String, String> properties = getProperties();
        properties.put(NUMERIC_PRECISION_MAPPING_CONFIG, Boolean.FALSE.toString());
        JdbcSourceTaskConfig config = new JdbcSourceTaskConfig(properties);
        ResultSetMetaData metadata = mockNumericSchema();
        Schema schema = new DataConverter(config).convertSchema("mock", metadata);
        List<Field> fields = schema.fields();
        assertEquals(metadata.getColumnCount(), fields.size());
        for (int i = 0; i < fields.size(); i++) {
            assertEquals(Schema.Type.BYTES, fields.get(i).schema().type());
            assertEquals((i % 2 != 0), fields.get(i).schema().isOptional());
        }
    }
}
