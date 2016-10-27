package org.embulk.parser.csv_with_default_value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

@JsonDeserialize(as = ColumnDefaultValueImpl.class)
public interface ColumnDefaultValue {

    Set<Type> ALLOWED_TYPES = ImmutableSet.<Type>of(Types.LONG, Types.DOUBLE, Types.DOUBLE, Types.TIMESTAMP);
    String ALLOWED_TYPES_NAME = Joiner.on(",").join(ALLOWED_TYPES);

    @Config("default_value")
    Optional<String> getDefaultValue();

    @Config("type")
    @ConfigDefault("immediate")
    ColumnDefaultValue.ValueType getType();

    interface DefaultValueSetter {

        /**
         * @throws CsvRecordValidateException
         */
        void longValue(ColumnDefaultValue value, PageBuilder pageBuilder, Column column);

        /**
         * @throws CsvRecordValidateException
         */
        void doubleValue(ColumnDefaultValue value, PageBuilder pageBuilder, Column column);

        /**
         * @throws CsvRecordValidateException
         */
        void timestampValue(ColumnDefaultValue value, TimestampParser parser, PageBuilder pageBuilder, Column column);

    }

    enum ValueType implements DefaultValueSetter {
        IMMEDIATE {
            @Override
            public void longValue(ColumnDefaultValue value, PageBuilder pageBuilder, Column column) {
                try {
                    pageBuilder.setLong(column, Long.parseLong(value.getDefaultValue().get()));
                } catch (NumberFormatException e) {
                    throw new CsvRecordValidateException(e);
                }
            }

            @Override
            public void doubleValue(ColumnDefaultValue value, PageBuilder pageBuilder, Column column) {
                try {
                    pageBuilder.setDouble(column, Double.parseDouble(value.getDefaultValue().get()));
                } catch (NumberFormatException e) {
                    throw new CsvRecordValidateException(e);
                }

            }

            @Override
            public void timestampValue(ColumnDefaultValue value, TimestampParser parser, PageBuilder pageBuilder, Column column) {
                try {
                    pageBuilder.setTimestamp(column, parser.parse(value.getDefaultValue().get()));
                } catch (TimestampParseException e) {
                    throw new CsvRecordValidateException(e);
                }
            }
        },
        NULL {
            @Override
            public void longValue(ColumnDefaultValue value, PageBuilder pageBuilder, Column column) {
                throw new ConfigException("null value is not allowed for long");
            }

            @Override
            public void doubleValue(ColumnDefaultValue value, PageBuilder pageBuilder, Column column) {
                throw new ConfigException("null value is not allowed for double");
            }

            @Override
            public void timestampValue(ColumnDefaultValue value, TimestampParser parser, PageBuilder pageBuilder, Column column) {
                pageBuilder.setNull(column);
            }
        };

        @JsonValue
        @Override
        public String toString() {
            return name().toLowerCase(Locale.ENGLISH);
        }


        @JsonCreator
        public static ValueType fromString(String value) {
            Map<String, ValueType> types = ImmutableMap.of(IMMEDIATE.toString().toLowerCase(), IMMEDIATE, NULL.toString().toLowerCase(), NULL);
            ValueType type = types.get(value);
            if (type != null) {
                return type;
            } else {
                throw new ConfigException(String.format("Unknown value_type '%s', Supported getType are immediate, null.", value));
            }
        }
    }
}


