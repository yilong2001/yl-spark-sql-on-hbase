package com.example.spark.demo.impl.transfer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.example.spark.demo.impl.transfer.SplitCharTransfer.SPLICT_CHAR_FLAG;

/**
 * Created by yilong on 2018/6/11.
 */
public class ConcatTransfer extends RowKeyTransfer implements Serializable {
    private RowKeyTransfer[] transfers;

    public static String CONCAT_CHAR_FLAG = "_";

    public ConcatTransfer(RowKeyTransfer[] transfers) {
        this.transfers = transfers;
    }

    @Override
    public String transfer(Object element) {
        String out = "";
        boolean first = true;
        for (RowKeyTransfer tf : transfers) {
            if (first) {
                out += tf.transfer(element);
                first = false;
            } else {
                out += CONCAT_CHAR_FLAG + tf.transfer(element);
            }
        }

        return out;
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        String out = "";
        boolean first = true;
        for (RowKeyTransfer tf : transfers) {
            String value = tf.transferPartial(partialFieldValues);
            if (value == null) {
                System.out.println(out + " : break");
                break;
            }

            if (first) {
                out += value;
                first = false;
            } else {
                out += CONCAT_CHAR_FLAG + value;
            }
        }

        return out;
    }

    @Override
    public String rowkeyFields() {
        String out = "";
        for (RowKeyTransfer tf : transfers) {
            if (out.length() == 0) {
                out += tf.rowkeyFields();
            } else {
                out += CONCAT_CHAR_FLAG+tf.rowkeyFields();
            }
        }

        return out;
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        for (RowKeyTransfer tf : transfers) {
            switch (tf.fieldSortType(field)) {
                case ASC: return RowkeyFieldSortType.ASC;
                case DESC: return RowkeyFieldSortType.ASC;
                case NONE: ;
            }
        }

        return RowkeyFieldSortType.NONE;
    }
}
