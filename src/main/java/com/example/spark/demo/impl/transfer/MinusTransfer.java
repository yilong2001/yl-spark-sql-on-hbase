package com.example.spark.demo.impl.transfer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2018/6/11.
 */
public class MinusTransfer extends RowKeyTransfer implements Serializable {
    private RowKeyTransfer transfer;
    private Long base;
    public MinusTransfer(Long base, RowKeyTransfer  transfer) {
        this.transfer = transfer;
        this.base = base;
    }

    @Override
    public String transfer(Object element) {
        try {
            long x = Long.parseLong(transfer.transfer(element));
            return ""+(base - x);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return ""+base;
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        try {
            String value = transfer.transferPartial(partialFieldValues);
            if (value == null) {
                System.out.println("transfers result is null");
                return null;
            }
            long x = Long.parseLong(value);
            return ""+(base - x);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return ""+base;
    }

    @Override
    public String rowkeyFields() {
        return transfer.rowkeyFields();
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        switch (transfer.fieldSortType(field)) {
            case ASC: return RowkeyFieldSortType.DESC;
            case DESC: return RowkeyFieldSortType.ASC;
            case NONE: ;
        }

        return RowkeyFieldSortType.NONE;
    }
}
