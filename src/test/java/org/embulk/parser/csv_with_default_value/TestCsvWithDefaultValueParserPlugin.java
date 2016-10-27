package org.embulk.parser.csv_with_default_value;

import com.google.common.collect.Maps;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.nio.charset.Charset;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.util.Newline;
import org.embulk.EmbulkTestRuntime;


public class TestCsvWithDefaultValueParserPlugin {


    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "date_code",
                                "type", "string"))
                );

        CsvWithDefaultValueParserPlugin.PluginTask task = config.loadConfig(CsvWithDefaultValueParserPlugin.PluginTask.class);
        assertEquals(Charset.forName("utf-8"), task.getCharset());
        assertEquals(Newline.CRLF, task.getNewline());
        assertEquals(false, task.getHeaderLine().or(false));
        assertEquals(",", task.getDelimiter());
        assertEquals(Optional.of(new CsvWithDefaultValueParserPlugin.QuoteCharacter('\"')), task.getQuoteChar());
        assertEquals(false, task.getAllowOptionalColumns());
        assertEquals(DateTimeZone.UTC, task.getDefaultTimeZone());
        assertEquals("%Y-%m-%d %H:%M:%S.%N %z", task.getDefaultTimestampFormat());
        assertEquals(Maps.newHashMap(), task.getDefaultValues());
    }
    @Test(expected = ConfigException.class)
    public void checkColumnsRequired()
    {
        ConfigSource config = Exec.newConfigSource();

        config.loadConfig(CsvWithDefaultValueParserPlugin.PluginTask.class);
    }

    @Test
    public void checkLoadConfig()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("charset", "utf-16")
                .set("newline", "LF")
                .set("header_line", true)
                .set("delimiter", "\t")
                .set("quote", "\\")
                .set("allow_optional_columns", true)
                .set("columns", ImmutableList.of(
                        ImmutableMap.of(
                                "name", "date_code",
                                "type", "string"))

                )
                .set("default_values", ImmutableMap.of(
                        "longCol", ImmutableMap.of("default_value", 123, "type", "immediate"),
                        "doubleCol", ImmutableMap.of("default_value", 123, "type", "immediate"),
                        "timestampCol", ImmutableMap.of("default_value", "1900-01-01 00:00:00", "type", "immediate")
                ));

        CsvWithDefaultValueParserPlugin.PluginTask task = config.loadConfig(CsvWithDefaultValueParserPlugin.PluginTask.class);
        assertEquals(Charset.forName("utf-16"), task.getCharset());
        assertEquals(Newline.LF, task.getNewline());
        assertEquals(true, task.getHeaderLine().or(false));
        assertEquals("\t", task.getDelimiter());
        assertEquals(Optional.of(new CsvWithDefaultValueParserPlugin.QuoteCharacter('\\')), task.getQuoteChar());
        assertEquals(true, task.getAllowOptionalColumns());

        assertEquals(ImmutableMap.of(
                "longCol", new ColumnDefaultValueImpl("123", ColumnDefaultValue.ValueType.IMMEDIATE),
                "doubleCol", new ColumnDefaultValueImpl("123", ColumnDefaultValue.ValueType.IMMEDIATE),
                "timestampCol", new ColumnDefaultValueImpl("1900-01-01 00:00:00", ColumnDefaultValue.ValueType.IMMEDIATE)),
                task.getDefaultValues());
    }

    @Test
    public void checkColumnDefaultValues(){

    }


}
