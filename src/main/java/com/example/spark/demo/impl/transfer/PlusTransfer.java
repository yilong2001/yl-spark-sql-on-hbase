package com.example.spark.demo.impl.transfer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2018/6/11.
 */
public class PlusTransfer extends RowKeyTransfer implements Serializable {
    private RowKeyTransfer[] transfers;
    public PlusTransfer(RowKeyTransfer[] transfers) {
        this.transfers = transfers;
    }

    @Override
    public String transfer(Object element) {
        List<Long> nums = new ArrayList<>();

        for (RowKeyTransfer tf : transfers) {
            try {
                long x = Long.parseLong(tf.transfer(element));
                nums.add(x);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        long out = 0;
        for (long x : nums) {
            out += x;
        }

        return ""+out;
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        List<Long> nums = new ArrayList<>();

        for (RowKeyTransfer tf : transfers) {
            try {
                String value = tf.transferPartial(partialFieldValues);
                if (value == null) {
                    System.out.println("transfers result is null");
                    break;
                }
                long x = Long.parseLong(value);
                nums.add(x);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        if (nums.size() == 0) {
            System.out.println("transferPartial is null");
            return null;
        }

        long out = 0;
        for (long x : nums) {
            out += x;
        }

        return ""+out;
    }

    @Override
    public String rowkeyFields() {
        String out = "";
        for (RowKeyTransfer tf : transfers) {
            if (out.length() == 0) {
                out += tf.rowkeyFields();
            } else {
                out += "."+tf.rowkeyFields();
            }
        }

        return out;
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        for (RowKeyTransfer tf : transfers) {
            switch (tf.fieldSortType(field)) {
                case ASC: return RowkeyFieldSortType.ASC;
                case DESC: return RowkeyFieldSortType.DESC;
                case NONE: ;
            }
        }

        return RowkeyFieldSortType.NONE;
    }
}
