package org.embulk.parser.csv_with_default_value;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import org.embulk.config.Task;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.util.LineDecoder;
import org.embulk.spi.util.Timestamps;
import org.slf4j.Logger;

import java.util.Map;

public class CsvWithDefaultValueParserPlugin
        implements ParserPlugin
{
    private static final ImmutableSet<String> TRUE_STRINGS =
            ImmutableSet.of(
                    "true", "True", "TRUE",
                    "yes", "Yes", "YES",
                    "t", "T", "y", "Y",
                    "on", "On", "ON",
                    "1");

    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.Task
    {
        @Config("columns")
        SchemaConfig getSchemaConfig();

        @Config("header_line")
        @ConfigDefault("null")
        Optional<Boolean> getHeaderLine();

        @Config("skip_header_lines")
        @ConfigDefault("0")
        int getSkipHeaderLines();
        void setSkipHeaderLines(int n);

        @Config("delimiter")
        @ConfigDefault("\",\"")
        String getDelimiter();

        @Config("quote")
        @ConfigDefault("\"\\\"\"")
        Optional<QuoteCharacter> getQuoteChar();

        @Config("escape")
        @ConfigDefault("\"\\\\\"")
        Optional<EscapeCharacter> getEscapeChar();

        // Null value handling: if the CsvParser found 'non-quoted empty string's,
        // it replaces them to string that users specified like "\N", "NULL".
        @Config("null_string")
        @ConfigDefault("null")
        Optional<String> getNullString();

        @Config("trim_if_not_quoted")
        @ConfigDefault("false")
        boolean getTrimIfNotQuoted();

        @Config("max_quoted_size_limit")
        @ConfigDefault("131072") //128kB
        long getMaxQuotedSizeLimit();

        @Config("comment_line_marker")
        @ConfigDefault("null")
        Optional<String> getCommentLineMarker();

        @Config("allow_optional_columns")
        @ConfigDefault("false")
        boolean getAllowOptionalColumns();

        @Config("allow_extra_columns")
        @ConfigDefault("false")
        boolean getAllowExtraColumns();

        @Config("stop_on_invalid_record")
        @ConfigDefault("false")
        boolean getStopOnInvalidRecord();

        @Config("default_values")
        @ConfigDefault("{}")
        Map<String, ColumnDefaultValue> getDefaultValues();

    }

    public static class QuoteCharacter
    {
        private final char character;

        public QuoteCharacter(char character)
        {
            this.character = character;
        }

        public static QuoteCharacter noQuote()
        {
            return new QuoteCharacter(CsvTokenizer.NO_QUOTE);
        }

        @JsonCreator
        public static QuoteCharacter ofString(String str)
        {
            if (str.length() >= 2) {
                throw new ConfigException("\"quote\" option accepts only 1 character.");
            } else if (str.isEmpty()) {
                Exec.getLogger(CsvWithDefaultValueParserPlugin.class).warn("Setting '' (empty string) to \"quote\" option is obsoleted. Currently it becomes '\"' automatically but this behavior will be removed. Please set '\"' explicitly.");
                return new QuoteCharacter('"');
            } else {
                return new QuoteCharacter(str.charAt(0));
            }
        }

        @JsonIgnore
        public char getCharacter()
        {
            return character;
        }

        @JsonValue
        public String getOptionalString()
        {
            return new String(new char[] { character });
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof QuoteCharacter)) {
                return false;
            }
            QuoteCharacter o = (QuoteCharacter) obj;
            return character == o.character;
        }
    }

    public static class EscapeCharacter
    {
        private final char character;

        public EscapeCharacter(char character)
        {
            this.character = character;
        }

        public static EscapeCharacter noEscape()
        {
            return new EscapeCharacter(CsvTokenizer.NO_ESCAPE);
        }

        @JsonCreator
        public static EscapeCharacter ofString(String str)
        {
            if (str.length() >= 2) {
                throw new ConfigException("\"escape\" option accepts only 1 character.");
            } else if (str.isEmpty()) {
                Exec.getLogger(CsvWithDefaultValueParserPlugin.class).warn("Setting '' (empty string) to \"escape\" option is obsoleted. Currently it becomes null automatically but this behavior will be removed. Please set \"escape: null\" explicitly.");
                return noEscape();
            } else {
                return new EscapeCharacter(str.charAt(0));
            }
        }

        @JsonIgnore
        public char getCharacter()
        {
            return character;
        }

        @JsonValue
        public String getOptionalString()
        {
            return new String(new char[] { character });
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof EscapeCharacter)) {
                return false;
            }
            EscapeCharacter o = (EscapeCharacter) obj;
            return character == o.character;
        }
    }

    private final Logger log;

    public CsvWithDefaultValueParserPlugin()
    {
        log = Exec.getLogger(CsvWithDefaultValueParserPlugin.class);
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // backward compatibility
        if (task.getHeaderLine().isPresent()) {
            if (task.getSkipHeaderLines() > 0) {
                throw new ConfigException("'header_line' option is invalid if 'skip_header_lines' is set.");
            }
            if (task.getHeaderLine().get()) {
                task.setSkipHeaderLines(1);
            } else {
                task.setSkipHeaderLines(0);
            }
        }

        control.run(task.dump(), task.getSchemaConfig().toSchema());
    }

    @Override
    public void run(TaskSource taskSource, final Schema schema,
                    FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final TimestampParser[] timestampParsers = Timestamps.newTimestampColumnParsers(task, task.getSchemaConfig());
        final JsonParser jsonParser = new JsonParser();
        final CsvTokenizer tokenizer = new CsvTokenizer(new LineDecoder(input, task), task);
        final boolean allowOptionalColumns = task.getAllowOptionalColumns();
        final boolean allowExtraColumns = task.getAllowExtraColumns();
        final boolean stopOnInvalidRecord = task.getStopOnInvalidRecord();
        int skipHeaderLines = task.getSkipHeaderLines();

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            while (tokenizer.nextFile()) {
                // skip the header lines for each file
                for (; skipHeaderLines > 0; skipHeaderLines--) {
                    if (!tokenizer.skipHeaderLine()) {
                        break;
                    }
                }

                if (!tokenizer.nextRecord()) {
                    // empty file
                    continue;
                }

                while (true) {
                    boolean hasNextRecord;

                    try {
                        schema.visitColumns(new DefaultValueAwareColumnVisitor(pageBuilder, task, tokenizer, timestampParsers));

                        try {
                            hasNextRecord = tokenizer.nextRecord();
                        } catch (CsvTokenizer.TooManyColumnsException ex) {
                            if (allowExtraColumns) {
                                String tooManyColumnsLine = tokenizer.skipCurrentLine();
                                // TODO warning
                                hasNextRecord = tokenizer.nextRecord();
                            } else {
                                // this line will be skipped at the following catch section
                                throw ex;
                            }
                        }
                        pageBuilder.addRecord();

                    } catch (CsvTokenizer.InvalidFormatException | CsvTokenizer.InvalidValueException | CsvRecordValidateException e) {
                        String skippedLine = tokenizer.skipCurrentLine();
                        long lineNumber = tokenizer.getCurrentLineNumber();
                        if (stopOnInvalidRecord) {
                            throw new DataException(String.format("Invalid record at line %d: %s", lineNumber, skippedLine), e);
                        }
                        log.warn(String.format("Skipped line %d (%s): %s", lineNumber, e.getMessage(), skippedLine));
                        //exec.notice().skippedLine(skippedLine);

                        hasNextRecord = tokenizer.nextRecord();
                    }

                    if (!hasNextRecord) {
                        break;
                    }
                }
            }

            pageBuilder.finish();
        }

    }

    static class DefaultValueAwareColumnVisitor implements ColumnVisitor {

        private final PageBuilder pageBuilder;
        private final PluginTask task;
        private final TimestampParser[] timestampParsers;
        private final JsonParser jsonParser;
        private final boolean allowOptionalColumns;
        private final CsvTokenizer tokenizer;
        private final Logger log = Exec.getLogger(CsvWithDefaultValueParserPlugin.class);

        DefaultValueAwareColumnVisitor(PageBuilder pageBuilder, PluginTask task, CsvTokenizer tokenizer, TimestampParser[] timestampParsers) {
            this.pageBuilder = pageBuilder;
            this.timestampParsers = timestampParsers;
            this.jsonParser = new JsonParser();
            this.tokenizer = tokenizer;
            this.allowOptionalColumns = task.getAllowOptionalColumns();
            this.task = task;
            assertDefaultValuesAreAllowedForTypes();
        }

        private void assertDefaultValuesAreAllowedForTypes(){
            for(Map.Entry<String, ColumnDefaultValue> e: task.getDefaultValues().entrySet()){
                ColumnConfig col = task.getSchemaConfig().lookupColumn(e.getKey());
                if(col == null){
                    throw new ConfigException(String.format("column %s is not found.", e.getKey()));
                }else if(!ColumnDefaultValue.ALLOWED_TYPES.contains(col.getType())){
                    throw new ConfigException(String.format("default value are allowed for only %s", ColumnDefaultValue.ALLOWED_TYPES_NAME));
                }
            }
        }

        public void booleanColumn(Column column)
        {
            String v = nextColumn();
            if (v == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setBoolean(column, TRUE_STRINGS.contains(v));
            }
        }

        public void longColumn(Column column)
        {
            String v = nextColumn();
            if (v == null) {
                pageBuilder.setNull(column);
            } else {
                try {
                    pageBuilder.setLong(column, Long.parseLong(v));
                } catch (NumberFormatException e) {
                    final Optional<ColumnDefaultValue> defaultValue = getDefaultValue(task, column);
                    if(defaultValue.isPresent()){
                        defaultValue.get().getType().longValue(defaultValue.get(), pageBuilder, column);
                        log.warn(String.format("Applying default value due to fail to parse: %s(%s)", v, column.getName()));
                    }else {
                        throw new CsvRecordValidateException(e);
                    }

                }
            }
        }

        public void doubleColumn(Column column)
        {
            String v = nextColumn();
            if (v == null) {
                pageBuilder.setNull(column);
            } else {
                try {
                    pageBuilder.setDouble(column, Double.parseDouble(v));
                } catch (NumberFormatException e) {
                    final Optional<ColumnDefaultValue> defaultValue = getDefaultValue(task, column);
                    if(defaultValue.isPresent()){
                        defaultValue.get().getType().doubleValue(defaultValue.get(), pageBuilder, column);
                        log.warn(String.format("Applying default value due to fail to parse: %s(%s)", v, column.getName()));
                    }else {
                        throw new CsvRecordValidateException(e);
                    }                                    }
            }
        }

        public void stringColumn(Column column)
        {
            String v = nextColumn();
            if (v == null) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setString(column, v);
            }
        }

        public void timestampColumn(Column column)
        {
            String v = nextColumn();
            if (v == null) {
                pageBuilder.setNull(column);
            } else {
                try {
                    pageBuilder.setTimestamp(column, timestampParsers[column.getIndex()].parse(v));
                } catch (TimestampParseException e) {
                    final Optional<ColumnDefaultValue> defaultValue = getDefaultValue(task, column);
                    if(defaultValue.isPresent()){
                        defaultValue.get().getType().timestampValue(defaultValue.get(), timestampParsers[column.getIndex()], pageBuilder, column);
                        log.warn(String.format("Applying default value due to fail to parse: %s(%s)", v, column.getName()));
                    }else{
                        throw new CsvRecordValidateException(e);
                    }

                }
            }
        }

        public void jsonColumn(Column column)
        {
            String v = nextColumn();
            if (v == null) {
                pageBuilder.setNull(column);
            } else {
                try {
                    pageBuilder.setJson(column, jsonParser.parse(v));
                } catch (JsonParseException e) {
                    // TODO support default value
                    throw new CsvRecordValidateException(e);
                }
            }
        }

        private String nextColumn()
        {
            if (allowOptionalColumns && !tokenizer.hasNextColumn()) {
                //TODO warning
                return null;
            }
            return tokenizer.nextColumnOrNull();
        }

        protected Optional<ColumnDefaultValue> getDefaultValue(final PluginTask task, final Column column){
            final ColumnDefaultValue value = task.getDefaultValues().get(column.getName());
            if(value == null){
                return Optional.absent();
            }
            //Check values is set if immediate
            if(value.getType() == ColumnDefaultValue.ValueType.IMMEDIATE && !value.getDefaultValue().isPresent()){
                throw new ConfigException(String.format("default_value is not set to column '%s'", column.getName()));
            }else if(value.getType() == ColumnDefaultValue.ValueType.NULL && value.getDefaultValue().isPresent()){
                throw new ConfigException(String.format("default_value is set to column '%s', even though type is null.", column.getName()));
            }
            return Optional.of(value);
        }

    }
}