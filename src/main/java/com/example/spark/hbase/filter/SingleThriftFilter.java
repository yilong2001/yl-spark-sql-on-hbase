package com.example.spark.hbase.filter;

import com.example.spark.demo.impl.cmp.*;
import com.example.spark.sql.util.ThriftSerde;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by yilong on 2018/6/10.
 */
public class SingleThriftFilter extends FilterBase implements Serializable {
    private static Logger logger = Logger.getLogger(SingleThriftFilter.class);

    //TODO: 暂时考虑一个列簇的情况
    //private byte[] targetFamily;
    private String targetFamily;
    //TODO: 过滤目标字段:暂不考虑

    private Filter rowfilter = null;

    private Class cls;

    private ValueCondition valueCondition;

    public SingleThriftFilter(byte[] targetFamily,
                              Filter rowfilter,
                              ValueCondition valueCondition,
                              Class cls) {
        this.targetFamily = Bytes.toStringBinary(targetFamily);

        this.rowfilter = rowfilter;

        this.cls = cls;

        this.valueCondition = valueCondition;
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        String f = Bytes.toStringBinary(cell.getFamilyArray());
        if (!f.equals(targetFamily)) {
            return ReturnCode.NEXT_ROW;
        }

        //TODO:

        Object obj = ThriftSerde.deSerialize(cls, cell.getValueArray());

        if (valueCondition != null) {
            if (valueCondition.isMatched(obj)) return ReturnCode.INCLUDE;
            return ReturnCode.NEXT_ROW;
        } else {
            return ReturnCode.INCLUDE;
        }
    }

    /**
     */
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        logger.info("filterRowKey : " + offset +"; " + length);
        return rowfilter.filterRowKey(buffer, offset, length);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(this.valueCondition);

        byte[] vcbs = out.toByteArray();
        byte[] fbs = rowfilter.toByteArray();

        byte[] len1 = intToByteArray(((int)vcbs.length));
        byte[] len2 = intToByteArray(((int)fbs.length));

        byte[] newStr = new byte[4+4+vcbs.length+fbs.length];

        for (int i=0;i<4;i++) {
            newStr[i] = len1[i];
        }
        for (int i=0;i<vcbs.length;i++) {
            newStr[4+i] = vcbs[i];
        }
        for (int i=0;i<4;i++) {
            newStr[4+vcbs.length+i] = len2[i];
        }
        for (int i=0;i<fbs.length;i++) {
            newStr[8+vcbs.length+i] = fbs[i];
        }


        return newStr;
    }

    public static SingleThriftFilter parseFrom(final byte[] data) {
        byte[] len1 = subBytes(data, 0, 4);
        int len1ln = byteArrayToInt(len1);
        byte[] vcbs = subBytes(data, 4, len1ln);

        byte[] len2 = subBytes(data, 4+len1ln, 4);
        int len2ln = byteArrayToInt(len2);
        byte[] fbs = subBytes(data, 8+len1ln, len2ln);

        Filter rowFilter = null;
        try {
            rowFilter = RowFilter.parseFrom(fbs);
        } catch (DeserializationException e) {
            logger.error(e.getMessage(), e);
        }

        ValueCondition vc = null;
        try {
            ByteArrayInputStream inb = new ByteArrayInputStream(vcbs);
            ObjectInputStream os = new ObjectInputStream(inb);
            vc = (ValueCondition)os.readObject();
        } catch (Exception e) {

        }

        SingleThriftFilter singleThriftFilter = new SingleThriftFilter("f".getBytes(), rowFilter, vc, null);

        return singleThriftFilter;
    }

    public static byte[] subBytes(byte[] src, int begin, int count) {
        byte[] bs = new byte[count];
        System.arraycopy(src, begin, bs, 0, count);
        return bs;
    }

    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public String getTargetFamily() {
        return targetFamily;
    }

    public void setTargetFamily(String targetFamily) {
        this.targetFamily = targetFamily;
    }

    public Filter getRowfilter() {
        return rowfilter;
    }

    public void setRowfilter(Filter rowfilter) {
        this.rowfilter = rowfilter;
    }

    public ValueCondition getValueCondition() {
        return valueCondition;
    }

    public void setValueCondition(ValueCondition valueCondition) {
        this.valueCondition = valueCondition;
    }
}
