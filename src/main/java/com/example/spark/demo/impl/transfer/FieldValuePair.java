package com.example.spark.demo.impl.transfer;

/**
 * Created by yilong on 2018/6/12.
 */
public class FieldValuePair {
    private String field;
    private Object value;
    private FieldCompareType compareType;

    public enum FieldCompareType {
        Cmp_Equal,
        Cmp_Greater,Cmp_Less,
        Cmp_GreaterAndEqual,Cmp_LessAndEqual
    }

    public FieldValuePair(String field, Object value, FieldCompareType compareType) {
        this.field = field;
        this.value = value;
        this.compareType = compareType;
    }

    public FieldCompareType getCompareType() {
        return compareType;
    }

    public void setCompareType(FieldCompareType compareType) {
        this.compareType = compareType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
