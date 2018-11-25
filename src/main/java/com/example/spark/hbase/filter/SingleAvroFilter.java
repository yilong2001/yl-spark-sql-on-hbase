package com.example.spark.hbase.filter;

import com.example.spark.demo.impl.cmp.ValueCondition;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.List;

/**
 * Created by yilong on 2018/6/10.
 */
public class SingleAvroFilter extends FilterBase implements Serializable {
    private static Logger logger = Logger.getLogger(SingleAvroFilter.class);

    //TODO: 暂时考虑一个列簇的情况
    //private byte[] targetFamily;
    private String targetFamily;
    //TODO: 过滤目标字段:暂不考虑

    private Filter rowfilter = null;
    private String avsc;
    private ValueCondition valueCondition;

    private transient Schema schema;
    private transient DatumReader<GenericRecord> datumReader;

    public SingleAvroFilter(byte[] targetFamily,
                            Filter rowfilter,
                            ValueCondition valueCondition,
                            String avsc) {
        this.targetFamily = Bytes.toStringBinary(targetFamily);

        this.rowfilter = rowfilter;

        this.avsc = avsc;

        this.valueCondition = valueCondition;

        this.schema = new Schema.Parser().parse(avsc);

        this.datumReader = new GenericDatumReader<GenericRecord>(schema);
    }

    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        String f = Bytes.toStringBinary(cell.getFamily());
        logger.info("filterKeyValue start : (valueCondition != null) = " + (valueCondition != null)+", f="+f+", tf="+targetFamily);
        if (!f.equals(targetFamily)) {
            return ReturnCode.NEXT_ROW;
        }

        //TODO:
        Decoder decoder = DecoderFactory.get().binaryDecoder(cell.getValue(),null);
        GenericRecord result = datumReader.read(null,decoder);
        if (valueCondition != null) {
            boolean b = valueCondition.isMatched(result);
            logger.info("valueCondition match : " + b);
            logger.info(result);
            //logger.info(valueCondition.toString());
            if (b) return ReturnCode.INCLUDE;
            return ReturnCode.NEXT_ROW;
        } else {
            return ReturnCode.INCLUDE;
        }
    }

    private void resetFilters(Filter filter) throws IOException {
        if (filter == null) return;

        filter.reset();

        if (!(filter instanceof FilterList)) {
            return;
        }

        List<Filter> filters1 = ((FilterList)rowfilter).getFilters();
        for (Filter f1 : filters1) {
            f1.reset();
            if (f1 instanceof FilterList) {
                List<Filter> filters2 = ((FilterList)f1).getFilters();
                for (Filter f2 : filters2) {
                    f2.reset();
                }
            }
        }

        return;
    }

    /**
     */
    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        logger.info("filterRowKey : " + offset +"; " + length+"; (rowfilter == null) : " + (rowfilter == null));
        if (rowfilter == null) return false;

        //resetFilters(rowfilter);
        rowfilter.reset();

        boolean r = rowfilter.filterRowKey(buffer, offset, length);
        logger.info("filterRowKey[rowfilter.filterRowKey] : " + r);
        return r;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        byte[] vcbs = new byte[]{};

        if (this.valueCondition != null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(this.valueCondition);
            vcbs = out.toByteArray();
        }

        byte[] fbs = (rowfilter==null)?new byte[]{}:rowfilter.toByteArray();
        byte[] schs = avsc.getBytes();

        byte[] len1 = intToByteArray(((int)vcbs.length));
        byte[] len2 = intToByteArray(((int)fbs.length));
        byte[] len3 = intToByteArray(((int)schs.length));

        byte[] newStr = new byte[4+4+4+vcbs.length+fbs.length+schs.length];

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
        for (int i=0;i<4;i++) {
            newStr[8+vcbs.length+fbs.length+i] = len3[i];
        }
        for (int i=0;i<schs.length;i++) {
            newStr[12+vcbs.length+fbs.length+i] = schs[i];
        }

        return newStr;
    }

    public static SingleAvroFilter parseFrom(final byte[] data) {
        logger.info("SingleAvroFilter start ");

        byte[] len1 = subBytes(data, 0, 4);
        int len1ln = byteArrayToInt(len1);
        byte[] vcbs = subBytes(data, 4, len1ln);

        byte[] len2 = subBytes(data, 4+len1ln, 4);
        int len2ln = byteArrayToInt(len2);
        byte[] fbs = subBytes(data, 8+len1ln, len2ln);

        byte[] len3 = subBytes(data, 8+len1ln+len2ln, 4);
        int len3ln = byteArrayToInt(len3);
        byte[] schs = subBytes(data, 12+len1ln+len2ln, len3ln);

        Filter rowFilter = null;
        try {
            if (fbs.length > 0) {
                rowFilter = FilterList.parseFrom(fbs);
            }
        } catch (DeserializationException e) {
            logger.error(e.getMessage(), e);
        }

        ValueCondition vc = null;
        try {
            if (vcbs.length > 0) {
                ByteArrayInputStream inb = new ByteArrayInputStream(vcbs);
                ObjectInputStream os = new ObjectInputStream(inb);
                vc = (ValueCondition) os.readObject();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        String avs = new String(schs);

        SingleAvroFilter avroFilter = new SingleAvroFilter("f".getBytes(), rowFilter, vc, avs);

        return avroFilter;
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

    public String getAvsc() { return this.avsc; }
}
