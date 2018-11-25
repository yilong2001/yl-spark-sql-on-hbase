package com.example.spark.demo.client;

import com.example.spark.demo.impl.cmp.EqualCompator;
import com.example.spark.demo.impl.cmp.ValueCondition;
import com.example.spark.demo.generated.thrift.NodeInfo;
import com.example.spark.hbase.filter.SingleThriftFilter;
import com.example.spark.sql.util.ThriftSerde;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by yilong on 2018/6/10.
 */
public class ThriftClient {
    private static void saveInfo(HTable table) {
        Set<Long> ports = new HashSet<Long>();
        ports.add(1001L);
        ports.add(1002L);

        table.setAutoFlush(true);

        for (int i=0; i<10; i++) {
            Put put = new Put(("row"+i).getBytes());
            NodeInfo ni = new NodeInfo();
            ni.setNode("localhost:"+i);
            ni.setPort(ports);

            byte[] data1 = ThriftSerde.serialize(ni);
            put.addColumn("f".getBytes(), "f".getBytes(), data1);

            NodeInfo nni = ThriftSerde.deSerialize((new NodeInfo(null,null)).getClass(), data1);

            assert(ni.getNode().equals(nni.getNode()));
            assert(ni.getNode()!=null);
            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void getInfo(HTable table) throws IOException {
        String row = "row0";
        Get get = new Get(row.getBytes());
        Result result = table.get(get);
        List<Cell> cls = result.getColumnCells("f".getBytes(), "f".getBytes());
        byte[] data = cls.get(0).getValueArray();
        //data = []
        //byte[] data1 = {0,0,0,18,0,0,0,64,0,4,114,111,119,48,1,102,102,0,0,1,100,96,81,-31,-13,4,11,0,1,0,0,0,11,108,111,99,97,108,104,111,115,116,58,48,14,0,2,10,0,0,0,2,0,0,0,0,0,0,3,-23,0,0,0,0,0,0,3,-22,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        NodeInfo ni = ThriftSerde.deSerialize(NodeInfo.class, data);
        System.out.println(ni);
    }

    public static void QueryAll(HTable table) {
        try {
            Scan s = new Scan();
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator("row5".getBytes()));
            SingleThriftFilter filter = new SingleThriftFilter("f".getBytes(), rowFilter, null, NodeInfo.class);
            s.setFilter(filter);

            ResultScanner rs = table.getScanner(s);

            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (org.apache.hadoop.hbase.KeyValue keyValue : r.raw()) {
                    NodeInfo ni = ThriftSerde.deSerialize(NodeInfo.class, keyValue.getValue());
                    System.out.println(ni);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void checkThriftSerde() {
        NodeInfo ni = new NodeInfo();
        ni.setNode("localhost");
        ni.addToPort(1001);
        ni.addToPort(1002);

        byte[] data = ThriftSerde.serialize(ni);
        System.out.println(data);

        NodeInfo obj = ThriftSerde.deSerialize(NodeInfo.class, data);
        System.out.println(obj);
    }

    public static void main(String[] args) throws Exception {
        //checkThriftSerde();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator("row5".getBytes()));
        ValueCondition valueCondition = new EqualCompator("node", "localhost:0");
        SingleThriftFilter singleThriftFilter = new SingleThriftFilter("f".getBytes(), rowFilter, valueCondition, null);

        byte[] sers = singleThriftFilter.toByteArray();
        SingleThriftFilter singleThriftFilter1 = SingleThriftFilter.parseFrom(sers);

        System.out.println(singleThriftFilter1.getRowfilter());

        //System.exit(1);

        Configuration conf= HBaseConfiguration.create();
        Connection conn=null;
        //Table table  =null;
        HTable table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            //table =conn.getTable(TableName.valueOf("thrift_test1"));

            table = new HTable(conf,TableName.valueOf("thrift_test1"));
            //saveInfo(table);

            //getInfo(table);

            QueryAll(table);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(conn!=null) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    conn = null;
                }
            }

            if(table!=null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }finally{
                    table=null;
                }
            }
        }
    }
}
