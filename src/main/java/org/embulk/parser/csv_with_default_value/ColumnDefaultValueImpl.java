package org.embulk.parser.csv_with_default_value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class ColumnDefaultValueImpl implements ColumnDefaultValue {

    @JsonProperty("default_value")
    private Optional<String> defaultValue;

    @JsonProperty("type")
    private ValueType type;

    public ColumnDefaultValueImpl(){
        this(Optional.<String>absent(), ValueType.IMMEDIATE);
    }

    public ColumnDefaultValueImpl(String defaultValue, ValueType type) {
        this(Optional.of(defaultValue), type);
    }

    public ColumnDefaultValueImpl(Optional<String> defaultValue, ValueType type){
        this.defaultValue = defaultValue;
        this.type = type;
    }

    @Override
    public Optional<String> getDefaultValue() {
        return defaultValue;
    }

    @Override
    public ValueType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || ! (o instanceof ColumnDefaultValue)) return false;

        ColumnDefaultValue that = (ColumnDefaultValue) o;

        if(getDefaultValue().isPresent() != that.getDefaultValue().isPresent()){
            return false;
        }else if(getDefaultValue().isPresent() && that.getDefaultValue().isPresent()){
            if(!getDefaultValue().get().equals(that.getDefaultValue().get())){
                return false;
            }
            return getType() == that.getType();
        }else{
            return getType() == that.getType();
        }
    }

    @Override
    public int hashCode() {
        int result = getDefaultValue() != null ? getDefaultValue().hashCode() : 0;
        result = 31 * result + (getType() != null ? getType().hashCode() : 0);
        return result;
    }

    @Override
    public String toString(){
        return String.format("ColumnDefaultValueImpl(type=%s,value=%s)", getType(), getDefaultValue().or("null"));
    }

}
