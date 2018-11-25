package com.example.spark.demo.impl.transfer;

import java.util.ArrayList;
import java.util.List;

import static com.example.spark.demo.impl.transfer.ConcatTransfer.CONCAT_CHAR_FLAG;
import static com.example.spark.demo.impl.transfer.SplitCharTransfer.SPLICT_CHAR_FLAG;

/**
 * Created by yilong on 2018/6/11.
 */
public abstract class RowKeyTransfer {
    public abstract String transfer(Object element);
    public abstract String transferPartial(List<FieldValuePair> partialFieldValues);
    public abstract String rowkeyFields();
    public abstract RowkeyFieldSortType fieldSortType(String field);

    public String[] rowkeyFieldArray() {
        String[] tmp = rowkeyFields().split(SPLICT_CHAR_FLAG+"|"+CONCAT_CHAR_FLAG);
        List<String> tmp2 = new ArrayList<String>();
        for(String t : tmp) {
            if (t.length() > 0) {
                tmp2.add(t);
            }
        }
        String[] outarr = new String[tmp2.size()];
        return tmp2.toArray(outarr);
    }

    public enum RowkeyFieldSortType {
        NONE, ASC, DESC
    }
}
