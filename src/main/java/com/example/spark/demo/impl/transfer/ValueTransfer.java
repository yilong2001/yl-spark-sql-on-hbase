package com.example.spark.demo.impl.transfer;

import com.example.spark.sql.util.ORMUtils;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yilong on 2018/6/11.
 */
public class ValueTransfer extends RowKeyTransfer implements Serializable {
    private String field;

    public ValueTransfer() {}

    public ValueTransfer(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    @Override
    public String transfer(Object element) {
        return ORMUtils.getSqlFieldValue(element, field).toString();
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        FieldValuePair fv = partialFieldValues.get(0);
        if (field.equals(fv.getField())) {
            partialFieldValues.remove(0);
            partialFieldValues.add(fv);
            return fv.getValue().toString();
        }

        System.out.println("rowkey transfer break : " + field);
        return null;
    }

    @Override
    public String rowkeyFields() {
        return field;
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        if (field.equals(this.field)) {
            return RowkeyFieldSortType.ASC;
        }

        return RowkeyFieldSortType.NONE;
    }
}
