package org.embulk.parser.csv_with_default_value;

import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Types;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.embulk.parser.csv_with_default_value.ColumnDefaultValue.ValueType.*;
import static org.mockito.Mockito.*;
public class ValueTypeTest {

    @Test
    public void testFromString(){
        assertThat(ColumnDefaultValue.ValueType.fromString("immediate"), equalTo(ColumnDefaultValue.ValueType.IMMEDIATE));
        assertThat(ColumnDefaultValue.ValueType.fromString("null"), equalTo(ColumnDefaultValue.ValueType.NULL));
        //TODO add assert for timestamp
    }

    @Test(expected = ConfigException.class)
    public void testFromStringThrowsException(){
        ColumnDefaultValue.ValueType.fromString("hoge");
    }

    @Test
    public void testGetValueMethods(){
        PageBuilder pageBuilder = mock(PageBuilder.class);

        IMMEDIATE.doubleValue(new ColumnDefaultValueImpl("123", IMMEDIATE), pageBuilder, new Column(0, "doubleCol", Types.DOUBLE));
        IMMEDIATE.longValue(new ColumnDefaultValueImpl("123", IMMEDIATE), pageBuilder, new Column(1, "longCol", Types.LONG));
        verify(pageBuilder).setDouble(new Column(0, "doubleCol", Types.DOUBLE),123.0);
        verify(pageBuilder).setLong(new Column(1, "longCol", Types.LONG),123L);
    }

    @Test(expected = ConfigException.class)
    public void testGetLongValueFromNullFails(){
        NULL.longValue(new ColumnDefaultValueImpl("123", NULL), mock(PageBuilder.class), new Column(0, "", Types.DOUBLE));
    }

    @Test(expected = ConfigException.class)
    public void testGetDoubleValueFromNullFails(){
        NULL.doubleValue(new ColumnDefaultValueImpl("123", NULL), mock(PageBuilder.class), new Column(0, "", Types.DOUBLE));
    }

}