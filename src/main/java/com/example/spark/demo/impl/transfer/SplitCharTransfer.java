package com.example.spark.demo.impl.transfer;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yilong on 2018/6/11.
 */
public class SplitCharTransfer extends RowKeyTransfer implements Serializable {
    private String value;
    public static String SPLICT_CHAR_FLAG = "#";

    public SplitCharTransfer(String v) {
        this.value = v;
    }
    @Override
    public String transfer(Object element) {
        return value;
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        return value;
    }

    @Override
    public String rowkeyFields() {
        return SPLICT_CHAR_FLAG;
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        return RowkeyFieldSortType.NONE;
    }
}
