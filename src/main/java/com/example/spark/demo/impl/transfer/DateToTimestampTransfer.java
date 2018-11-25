package com.example.spark.demo.impl.transfer;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static com.example.spark.demo.impl.transfer.SplitCharTransfer.SPLICT_CHAR_FLAG;

/**
 * Created by yilong on 2018/6/11.
 */
public class DateToTimestampTransfer extends RowKeyTransfer implements Serializable {
    private RowKeyTransfer inputTransfer;

    public DateToTimestampTransfer(RowKeyTransfer transfer) {
        this.inputTransfer = transfer;
    }

    @Override
    public String transfer(Object element) {
        List<String> candidators = new ArrayList<>();
        candidators.add("yyyy-MM-dd hh:mm:ss,SSS");
        candidators.add("yyyy-MM-dd hh:mm:ss");
        candidators.add("yyyy-MM-dd");
        candidators.add("yyyyMMdd");
        candidators.add("yyyy/MM/dd");

        String value = inputTransfer.transfer(element);

        for (String df : candidators) {
            try {
                DateFormat dateFormat = new SimpleDateFormat(df);
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
                Date d = dateFormat.parse(value);
                return "" + d.getTime();
            } catch (Exception e) {
                System.out.println(df + " : " + e.getMessage());
            }
        }

        return ""+System.currentTimeMillis();
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        List<String> candidators = new ArrayList<>();
        candidators.add("yyyy-MM-dd hh:mm:ss,SSS");
        candidators.add("yyyy-MM-dd hh:mm:ss");
        candidators.add("yyyy-MM-dd");
        candidators.add("yyyyMMdd");
        candidators.add("yyyy/MM/dd");

        String value = inputTransfer.transferPartial(partialFieldValues);
        if (value == null) {
            System.out.println("inputTransfer return null ");
            return null;
        }

        for (String df : candidators) {
            try {
                DateFormat dateFormat = new SimpleDateFormat(df);
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
                Date d = dateFormat.parse(value);
                return "" + d.getTime();
            } catch (Exception e) {
                System.out.println(df + " : " + e.getMessage());
            }
        }

        return ""+System.currentTimeMillis();
    }

    @Override
    public String rowkeyFields() {
        return inputTransfer.rowkeyFields();
    }

    @Override
    public RowkeyFieldSortType fieldSortType(String field) {
        switch (inputTransfer.fieldSortType(field)) {
            case ASC: return RowkeyFieldSortType.ASC;
            case DESC: return RowkeyFieldSortType.ASC;
            case NONE: ;
        }

        return RowkeyFieldSortType.NONE;
    }
}
