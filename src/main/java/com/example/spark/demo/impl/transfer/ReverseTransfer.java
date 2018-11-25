package com.example.spark.demo.impl.transfer;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2018/6/11.
 */
public class ReverseTransfer extends RowKeyTransfer implements Serializable {
    private RowKeyTransfer inputTransfer;

    public ReverseTransfer(RowKeyTransfer transfer) {
        this.inputTransfer = transfer;
    }

    @Override
    public String transfer(Object element) {
        String value = inputTransfer.transfer(element);
        StringBuilder sb=new StringBuilder(value);
        sb.reverse();

        return sb.toString();
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        String value = inputTransfer.transferPartial(partialFieldValues);
        if (value == null) {
            System.out.println("transfer result is null");
            return null;
        }

        StringBuilder sb=new StringBuilder(value);
        sb.reverse();

        return sb.toString();
    }

    @Override
    public String rowkeyFields() {
        return inputTransfer.rowkeyFields();
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        switch (inputTransfer.fieldSortType(field)) {
            case ASC: return RowkeyFieldSortType.ASC;
            case DESC: return RowkeyFieldSortType.DESC;
            case NONE: ;
        }

        return RowkeyFieldSortType.NONE;
    }
}
