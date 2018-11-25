package com.example.spark.demo.impl.transfer;

import com.example.spark.sql.util.Serde;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yilong on 2018/6/11.
 */
public class TimeToTimestampTransfer extends RowKeyTransfer implements Serializable {
    private RowKeyTransfer inputTransfer;

    public TimeToTimestampTransfer() {}
    public TimeToTimestampTransfer(RowKeyTransfer transfer) {
        this.inputTransfer = transfer;
    }

    public RowKeyTransfer getInputTransfer() {
        return inputTransfer;
    }

    public void setInputTransfer(RowKeyTransfer inputTransfer) {
        this.inputTransfer = inputTransfer;
    }

    @Override
    public String transfer(Object element) {
        List<String> candidators = new ArrayList<>();
        candidators.add("hh:mm:ss");
        candidators.add("hhmmss");

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

        return ""+(System.currentTimeMillis());
    }

    @Override
    public String transferPartial(List<FieldValuePair> partialFieldValues) {
        List<String> candidators = new ArrayList<>();
        candidators.add("hh:mm:ss");
        candidators.add("hhmmss");

        String value = inputTransfer.transferPartial(partialFieldValues);
        if (value == null) {
            System.out.println("transferPartial result is null");
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

        return ""+(System.currentTimeMillis());
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

    @Override
    public String rowkeyFields() {
        return inputTransfer.rowkeyFields();
    }

    static class TestClass implements Serializable {
        public int x;
        public String y;
        public int getX() {return x;}
        public void setX(int x) {this.x = x;}
        public String getY() {return y;}
        public void setY(String y) {this.y = y;}
    }
    public static void main(String[] args) {
        TestClass tc = new TestClass();
        tc.setX(120000);tc.setY("a");

        ValueTransfer vf = new ValueTransfer("x");

        TimeToTimestampTransfer tf = new TimeToTimestampTransfer(vf);

        System.out.println(tf.transfer(tc));
        System.out.println(tf.rowkeyFields());

        try {
            String sd = Serde.serialize(tf);
            System.out.println("--------------------------------------------");
            System.out.println(sd);

            TimeToTimestampTransfer ntf = Serde.deSerialize(sd);
            System.out.println(ntf.transfer(tc));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error : "+e.getMessage());
        }
        System.out.println("------------ row key field ----------------------");

        ValueTransfer vf2 = new ValueTransfer("y");
        SplitCharTransfer vf3 = new SplitCharTransfer("_");
        ConcatTransfer vf5 = new ConcatTransfer(new RowKeyTransfer[]{vf, vf3,vf2});
        for (String t : vf5.rowkeyFieldArray()) {
            System.out.println(t);
        }
    }

    public static void main1(String[] args) {
        String value = "135900";
        try {
            DateFormat dateFormat = new SimpleDateFormat("hhmmss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
            Date d = dateFormat.parse(value);
            System.out.println( d.getTime());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
